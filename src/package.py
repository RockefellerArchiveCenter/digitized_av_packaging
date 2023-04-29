import logging
import os
import tarfile
from pathlib import Path
from shutil import rmtree

import bagit
import boto3
import ffmpeg
from asnake.aspace import ASpace
from asnake.utils import find_closest_value
from dateutil import parser, relativedelta

logging.basicConfig(level=os.environ.get('LOGGING_LEVEL', logging.INFO))
logging.getLogger("bagit").setLevel(logging.ERROR)


class Packager(object):

    def __init__(self, refid, rights_ids, tmp_dir, source_bucket, destination_bucket,
                 destination_bucket_video_mezzanine, destination_bucket_video_access,
                 destination_bucket_audio_access, destination_bucket_poster, sns_topic):
        self.refid = refid
        self.rights_ids = [r.strip() for r in rights_ids.split(',')]
        self.tmp_dir = tmp_dir
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.destination_bucket_video_mezzanine = destination_bucket_video_mezzanine
        self.destination_bucket_video_access = destination_bucket_video_access
        self.destination_bucket_audio_access = destination_bucket_audio_access
        self.destination_bucket_poster = destination_bucket_poster
        self.sns_topic = sns_topic
        self.sns = boto3.client(
            'sns',
            region_name=os.environ.get('AWS_REGION_NAME', 'us-east-1'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
        self.s3 = boto3.client(
            's3',
            region_name=os.environ.get('AWS_REGION_NAME', 'us-east-1'),
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
        self.as_client = ASpace(
            baseurl=os.environ.get('AS_BASEURL', 'http://localhos:4567'),
            username=os.environ.get('AS_USERNAME', 'admin'),
            password=os.environ.get('AS_PASSWORD', 'admin')
        ).client
        self.as_repo = os.environ.get('AS_REPO', '2')
        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=1024 * 25,
            max_concurrency=10,
            multipart_chunksize=1024 * 25,
            use_threads=True)
        logging.debug(self.__dict__)

    def run(self):
        """Main method, which calls all other methods."""
        logging.debug(
            f'Packaging started for package {self.refid}.')
        try:
            bag_dir = Path(self.tmp_dir, self.refid)
            downloaded = self.download_files(bag_dir)
            self.format = self.parse_format(downloaded)
            self.create_poster(bag_dir)
            self.deliver_derivatives()
            self.create_bag(bag_dir, self.rights_ids)
            compressed_path = self.compress_bag(bag_dir)
            self.deliver_package(compressed_path)
            self.cleanup_successful_job()
            self.deliver_success_notification()
            logging.info(
                f'{self.format} package {self.refid} successfully packaged.')
        except Exception as e:
            logging.error(e)
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
        file_list = list(Path(self.tmp_dir).glob(f"{bag_dir}/*"))
        logging.debug(file_list)
        return file_list

    def parse_format(self, file_list):
        """Parses format information from file list.

        Args:
            file_list (list of pathlib.Path instances): List of filepaths in a bag.
        """
        if len(file_list) == 2 and any(
                [f.suffix == '.mp3' for f in file_list]):
            return 'audio'
        elif len(file_list) == 3 and any([f.suffix == '.mp4' for f in file_list]):
            return 'video'
        raise Exception(f'Unrecognized package format for files {file_list}.')

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
        logging.debug('Poster file {poster} created.')

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
        logging.debug('Derivative files delivered.')

    def uri_from_refid(self, refid):
        """Uses the find_by_id endpoint in AS to return the URI of an archival object."""
        find_by_refid_url = f"repositories/{self.as_repo}/find_by_id/archival_objects?ref_id[]={refid}"
        results = self.as_client.get(find_by_refid_url).json()
        if len(results.get("archival_objects")) == 1:
            return results['archival_objects'][0]['ref']
        else:
            raise Exception("{} results found for search {}. Expected one result.".format(
                len(results.get("archival_objects")), find_by_refid_url))

    def format_aspace_date(self, dates):
        """Formats ASpace dates so that they can be parsed by Aquila.
        Assumes beginning of month or year if a start date, and end of month or
        year if an end date.

        Args:
            dates (dict): ArchivesSpace date JSON

        Returns:
            Tuple of a begin date and end date in format YYYY-MM-DD
        """
        begin_date = dates['begin']
        end_date = None
        if dates['date_type'] == 'single':
            end_date = begin_date
        else:
            end_date = dates['end']
        parsed_begin = parser.isoparse(begin_date)
        parsed_end = parser.isoparse(end_date)
        formatted_begin = parsed_begin.strftime('%Y-%m-%d')
        if len(end_date) == 4:
            formatted_end = (
                parsed_end + relativedelta.relativedelta(
                    month=12, day=31)).strftime('%Y-%m-%d')
        elif len(end_date) == 7:
            formatted_end = (
                parsed_end + relativedelta.relativedelta(
                    day=31)).strftime('%Y-%m-%d')
        else:
            formatted_end = end_date
        return formatted_begin, formatted_end

    def create_bag(self, bag_dir, rights_ids):
        """Creates a BagIt bag from a directory.

        Args:
            bag_dir (pathlib.Path): directory containing local files.
            rights_ids (list): List of rights IDs to apply to the package.
        """
        obj_uri = self.uri_from_refid(bag_dir)
        start_date, end_date = self.format_aspace_date(
            find_closest_value(obj_uri, 'dates', self.as_client))
        metadata = {
            'ArchivesSpace-URI': obj_uri,
            'Start-Date': start_date,
            'End-Date': end_date,
            'Origin': f'av_digitization_{self.format}',
            'Rights-ID': rights_ids}
        bagit.make_bag(bag_dir, metadata)
        logging.debug(
            f'Bag created from {bag_dir} with Rights IDs {rights_ids}.')

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
        logging.debug(f'Compressed bag {compressed_path} created.')
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
        logging.debug('Packaged delivered.')

    def cleanup_successful_job(self):
        """Remove artifacts from successful job."""
        to_delete = self.s3.list_objects_v2(
            Bucket=self.source_bucket,
            Prefix=self.refid)['Contents']
        self.s3.delete_objects(
            Bucket=self.source_bucket,
            Delete={'Objects': [{'Key': obj['Key']} for obj in to_delete]})
        logging.debug('Cleanup from successful job completed.')

    def cleanup_failed_job(self, bag_dir):
        """Remove artifacts from failed job.

        Args:
            bag_dir (pathlib.Path): directory containing local files.
        """
        if bag_dir.is_dir():
            rmtree(bag_dir)
        Path(f"{bag_dir}.tar.gz").unlink(missing_ok=True)
        logging.debug('Cleanup from failed job completed.')

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
        logging.debug('Success notification delivered.')

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
        logging.debug('Failure notification delivered.')


if __name__ == '__main__':
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
