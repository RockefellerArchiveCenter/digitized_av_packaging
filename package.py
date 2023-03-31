import os
import tarfile
from pathlib import Path
from shutil import rmtree

import bagit
import boto3
import ffmpeg


class Packager(object):

    def __init__(self, format, refid, rights_ids, tmp_dir, source_bucket, destination_bucket,
                 destination_bucket_video_mezzanine, destination_bucket_video_access,
                 destination_bucket_audio_access, destination_bucket_poster, sns_topic):
        self.format = format
        self.refid = refid
        self.rights_ids = rights_ids
        self.tmp_dir = tmp_dir
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.destination_bucket_video_mezzanine = destination_bucket_video_mezzanine
        self.destination_bucket_video_access = destination_bucket_video_access
        self.destination_bucket_audio_access = destination_bucket_audio_access
        self.destination_bucket_poster = destination_bucket_poster
        self.sns_topic = sns_topic
        if self.format not in ['audio', 'video']:
            raise Exception(f"Unable to process format {self.format}")
        self.sns = boto3.client(
            'sns',
            region_name=os.environ.get('AWS_REGION_NAME', 'us-east-1'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_ACCESS_KEY_ID'))
        self.s3 = boto3.client(
            's3',
            region_name=os.environ.get('AWS_REGION_NAME', 'us-east-1'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_ACCESS_KEY_ID'))
        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=1024 * 25,
            max_concurrency=10,
            multipart_chunksize=1024 * 25,
            use_threads=True)

    def run(self):
        """Main method, which calls all other methods."""
        try:
            bag_dir = Path(self.tmp_dir, self.refid)
            self.download_files(bag_dir)
            self.create_poster(bag_dir)
            self.deliver_derivatives()
            self.create_bag(bag_dir, self.rights_ids)
            compressed_path = self.compress_bag(bag_dir)
            self.deliver_package(compressed_path)
            self.cleanup_successful_job()
            self.deliver_success_notification()
        except Exception as e:
            self.cleanup_failed_job(bag_dir)
            self.deliver_failure_notification(e)

    def download_files(self, bag_dir):
        """Downloads files from S3 to local storage.

        Args:
            bag_dir (pathlib.Path): directory containing local files.
        """
        if not bag_dir.is_dir():
            bag_dir.mkdir()
        to_download = self.s3.list_objects_v2(
            Bucket=self.source_bucket,
            Prefix=self.refid)['Contents']
        for obj in to_download:
            filename = obj['Key']
            self.s3.download_file(
                self.source_bucket,
                filename,
                f"{self.tmp_dir}/{filename}",
                Config=self.transfer_config)
        return list(Path().glob(f"{bag_dir}/*"))

    def create_poster(self, bag_dir):
        """Creates a poster image from a video file.

        Args:
            bag_dir (pathlib.Path): directory containing local files.
        """
        if self.format == 'video':
            poster = Path(bag_dir, 'poster.png')
            (
                ffmpeg
                .input(Path(bag_dir, f'{self.refid}_a.mp4'))
                .filter('thumbnail', 300)
                .output(str(poster), loglevel="quiet", **{'frames:v': 1})
                .run()
            )

    def derivative_map(self):
        """Get information about derivatives to upload to S3.

        Returns:
            derivative_map (list of three-tuples): path, S3 bucket and mimetype of files.
        """
        bag_path = Path(self.tmp_dir, self.refid)
        if self.format == 'video':
            return [
                (bag_path / f"{self.refid}_me.mov",
                 self.destination_bucket_video_mezzanine,
                 "video/quicktime"),
                (bag_path / f"{self.refid}_a.mp4",
                 self.destination_bucket_video_access, "video/mp4"),
                (bag_path / "poster.png", self.destination_bucket_poster, "image/x-png")
            ]
        else:
            return [
                (bag_path / f"{self.refid}_a.mp3",
                 self.destination_bucket_audio_access, "audio/mpeg"),
            ]

    def deliver_derivatives(self):
        """Uploads derivatives to S3 buckets and deletes them from temporary storage."""
        to_upload = self.derivative_map()
        for obj_path, bucket, content_type in to_upload:
            self.s3.upload_file(
                str(obj_path),
                bucket,
                f"{self.refid}{obj_path.suffix}",
                ExtraArgs={'ContentType': content_type},
                Config=self.transfer_config)
            obj_path.unlink()

    def create_bag(self, bag_dir, rights_ids):
        """Creates a BagIt bag from a directory.

        Args:
            bag_dir (pathlib.Path): directory containing local files.
            rights_ids (list): List of rights IDs to apply to the package.
        """
        # TODO check metadata requirements
        metadata = {'RightsID': rights_ids}
        bagit.make_bag(bag_dir, metadata)

    def compress_bag(self, bag_dir):
        """Creates a compressed archive file from a bag.

        Args:
            bag_dir (pathlib.Path): directory containing local files.

        Returns:
            compressed_path (pathlib.Path): path of compressed archive.
        """
        compressed_path = Path(f"{bag_dir}.tar.gz")
        with tarfile.open(str(compressed_path), "w:gz") as tar:
            tar.add(bag_dir, arcname=Path(bag_dir).name)
        rmtree(bag_dir)
        return compressed_path

    def deliver_package(self, package_path):
        """Delivers packaged files to destination.

        Args:
            package_path (pathlib.Path): path of compressed archive to upload.
        """
        self.s3.upload_file(
            package_path,
            self.destination_bucket,
            package_path.name,
            ExtraArgs={'ContentType': 'application/gzip'},
            Config=self.transfer_config)
        package_path.unlink()

    def cleanup_successful_job(self):
        """Remove artifacts from successful job."""
        to_delete = self.s3.list_objects_v2(
            Bucket=self.source_bucket,
            Prefix=self.refid)['Contents']
        self.s3.delete_objects(
            Bucket=self.source_bucket,
            Delete={'Objects': [{'Key': obj['Key']} for obj in to_delete]})

    def cleanup_failed_job(self, bag_dir):
        """Remove artifacts from failed job.

        Args:
            bag_dir (pathlib.Path): directory containing local files.
        """
        if bag_dir.is_dir():
            rmtree(bag_dir)
        Path(f"{bag_dir}.tar.gz").unlink(missing_ok=True)

    def deliver_success_notification(self):
        """Sends notifications after successful run."""
        self.sns.publish(
            TopicArn=self.sns_topic,
            Message=f'{self.format} package {self.refid} successfully packaged',
            MessageAttributes={
                'format': {
                    'DataType': 'String',
                    'StringValue': self.format,
                },
                'refid': {
                    'DataType': 'String',
                    'StringValue': self.refid,
                },
                'service': {
                    'DataType': 'String',
                    'StringValue': 'digitized_av_packaging',
                },
                'outcome': {
                    'DataType': 'String',
                    'StringValue': 'SUCCESS',
                }
            })

    def deliver_failure_notification(self, exception):
        """"Sends notifications when run fails.

        Args:
            exception (Exception): the exception that was thrown.
        """
        self.sns.publish(
            TopicArn=self.sns_topic,
            Message=f'{self.format} package {self.refid} failed packaging',
            MessageAttributes={
                'format': {
                    'DataType': 'String',
                    'StringValue': self.format,
                },
                'refid': {
                    'DataType': 'String',
                    'StringValue': self.refid,
                },
                'service': {
                    'DataType': 'String',
                    'StringValue': 'digitized_av_packaging',
                },
                'outcome': {
                    'DataType': 'String',
                    'StringValue': 'FAILURE',
                },
                'message': {
                    'DataType': 'String',
                    'StringValue': str(exception),
                }
            })


if __name__ == '__main__':
    format = os.environ.get('FORMAT')
    refid = os.environ.get('REFID')
    rights_ids = os.environ.get('RIGHTS_IDS')
    tmp_dir = os.environ.get('TMP_DIR')
    source_bucket = os.environ.get('AWS_SOURCE_BUCKET')
    destination_bucket = os.environ.get('AWS_DESTINATION_BUCKET')
    destination_bucket_video_mezzanine = os.environ.get(
        'AWS_DESTINATION_BUCKET_VIDEO_MEZZANINE')
    destination_bucket_video_access = os.environ.get(
        'AWS_DESTINATION_BUCKET_VIDEO_ACCESS')
    destination_bucket_audio_access = os.environ.get(
        'AWS_DESTINATION_BUCKET_AUDIO_ACCESS')
    destination_bucket_poster = os.environ.get('AWS_DESTINATION_BUCKET_POSTER')
    sns_topic = os.environ.get('AWS_SNS_TOPIC')
    Packager(
        format,
        refid,
        rights_ids,
        tmp_dir,
        source_bucket,
        destination_bucket,
        destination_bucket_video_mezzanine,
        destination_bucket_video_access,
        destination_bucket_audio_access,
        destination_bucket_poster,
        sns_topic).run()
