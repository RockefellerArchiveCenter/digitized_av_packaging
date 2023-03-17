import os
import tarfile
from pathlib import Path
from shutil import rmtree

import bagit
import boto3
import ffmpeg


class Packager(object):

    def __init__(self, format, refid, rights_ids, tmp_dir, source_bucket, destination_bucket,
                 destination_bucket_moving_image_mezzanine, destination_bucket_moving_image_access, destination_bucket_audio_access, destination_bucket_poster):
        self.format = format
        self.refid = refid
        self.rights_ids = rights_ids
        self.tmp_dir = tmp_dir
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.destination_bucket_moving_image_mezzanine = destination_bucket_moving_image_mezzanine
        self.destination_bucket_moving_image_access = destination_bucket_moving_image_access
        self.destination_bucket_audio_access = destination_bucket_audio_access
        self.destination_bucket_poster = destination_bucket_poster
        if self.format not in ['audio', 'moving image']:
            raise Exception(f"Unable to process format {self.format}")
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
        bag_dir = Path(self.tmp_dir, self.refid)
        self.download_files(bag_dir)
        self.create_poster(bag_dir)
        self.deliver_derivatives()
        self.create_bag(bag_dir, self.rights_ids)
        compressed_path = self.compress_bag(bag_dir)
        self.deliver_package(compressed_path)

    def download_files(self, bag_dir):
        """Downloads files from S3 to local storage."""
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
        self.s3.delete_objects(
            Bucket=self.source_bucket,
            Delete={'Objects': [{'Key': obj['Key']} for obj in to_download]})
        return list(Path().glob(f"{bag_dir}/*"))

    def create_poster(self, bag_dir):
        """Creates a poster image from a video file."""
        if self.format == 'moving image':
            poster = Path(bag_dir, 'poster.png')
            (
                ffmpeg
                .input(Path(bag_dir, f'{self.refid}_a.mp4'))
                .filter('thumbnail', 300)
                .output(str(poster), **{'frames:v': 1})
                .run()
            )

    def derivative_map(self):
        """Get information about derivatives to upload to S3."""
        bag_path = Path(self.tmp_dir, self.refid)
        if self.format == 'moving image':
            return [
                (bag_path / f"{self.refid}_me.mov",
                 self.destination_bucket_moving_image_mezzanine,
                 "video/quicktime"),
                (bag_path / f"{self.refid}_a.mp4",
                 self.destination_bucket_moving_image_access, "video/mp4"),
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
        """Creates a BagIt bag from a directory."""
        # TODO check metadata requirements
        metadata = {'RightsID': rights_ids}
        bagit.make_bag(bag_dir, metadata)

    def compress_bag(self, bag_dir):
        """Creates a compressed archive file from a bag."""
        compressed_path = Path(f"{bag_dir}.tar.gz")
        with tarfile.open(str(compressed_path), "w:gz") as tar:
            tar.add(bag_dir, arcname=Path(bag_dir).name)
        rmtree(bag_dir)
        return compressed_path

    def deliver_package(self, package_path):
        """Delivers packaged files to destination."""
        self.s3.upload_file(
            package_path,
            self.destination_bucket,
            package_path.name,
            ExtraArgs={'ContentType': 'application/gzip'},
            Config=self.transfer_config)
        package_path.unlink()


if __name__ == '__main__':
    format = os.environ.get('FORMAT')
    refid = os.environ.get('REFID')
    rights_ids = os.environ.get('RIGHTS_IDS')
    tmp_dir = os.environ.get('TMP_DIR')
    source_bucket = os.environ.get('AWS_SOURCE_BUCKET')
    destination_bucket = os.environ.get('AWS_DESTINATION_BUCKET')
    destination_bucket_moving_image_mezzanine = os.environ.get(
        'AWS_DESTINATION_BUCKET_MOVING_IMAGE_MEZZANINE')
    destination_bucket_moving_image_access = os.environ.get(
        'AWS_DESTINATION_BUCKET_MOVING_IMAGE_ACCESS')
    destination_bucket_audio_access = os.environ.get(
        'AWS_DESTINATION_BUCKET_AUDIO_ACCESS')
    destination_bucket_poster = os.environ.get('AWS_DESTINATION_BUCKET_POSTER')
    Packager(
        format,
        refid,
        rights_ids,
        tmp_dir,
        source_bucket,
        destination_bucket,
        destination_bucket_moving_image_mezzanine,
        destination_bucket_moving_image_access,
        destination_bucket_audio_access,
        destination_bucket_poster).run()
