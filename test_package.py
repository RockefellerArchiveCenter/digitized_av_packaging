import json
from pathlib import Path
from shutil import copyfile, copytree, rmtree
from unittest.mock import patch

import bagit
import boto3
import pytest
from moto import mock_s3, mock_sns, mock_sqs
from moto.core import DEFAULT_ACCOUNT_ID

from package import Packager

DEFAULT_ARGS = ['audio', 'b90862f3baceaae3b7418c78f9d50d52', ["1", "2"], "tmp", "source", "destination",
                "destination_mi_mezz", "destination_mi_access", "destination_audio_access", "destination_poster", "topic"]
VIDEO_ARGS = ['video', '20f8da26e268418ead4aa2365f816a08', ["1", "2"], "tmp", "source", "destination",
              "destination_mi_mezz", "destination_mi_access", "destination_audio_access", "destination-poster", "topic"]


@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Fixture to create and tear down tmp dir before and after a test is run"""
    tmp_dir = Path(DEFAULT_ARGS[3])
    if not tmp_dir.is_dir():
        tmp_dir.mkdir()

    yield  # this is where the testing happens

    rmtree(DEFAULT_ARGS[3])


def test_init():
    """Test arguments are correctly parsed."""
    Packager(*DEFAULT_ARGS)

    invalid_args = ['text', 'b90862f3baceaae3b7418c78f9d50d52', ["1", "2"], "tmp", "source", "destination",
                    "destination_mi_mezz", "destination_mi_access", "destination_audio_access", "destination_poster", "topic"]

    with pytest.raises(Exception, match="Unable to process format text"):
        Packager(*invalid_args)


@patch('package.Packager.download_files')
@patch('package.Packager.create_poster')
@patch('package.Packager.deliver_derivatives')
@patch('package.Packager.create_bag')
@patch('package.Packager.compress_bag')
@patch('package.Packager.deliver_package')
@patch('package.Packager.cleanup_successful_job')
@patch('package.Packager.deliver_success_notification')
def test_run(mock_notification, mock_cleanup, mock_deliver, mock_compress, mock_create,
             mock_deliver_derivatives, mock_poster, mock_download):
    """Asserts run method calls other methods."""
    packager = Packager(*DEFAULT_ARGS)
    bag_dir = Path(packager.tmp_dir, packager.refid)
    compressed_name = "foo.tar.gz"
    mock_compress.return_value = compressed_name
    packager.run()
    mock_cleanup.assert_called_once_with()
    mock_notification.assert_called_once_with()
    mock_deliver.assert_called_once_with(compressed_name)
    mock_compress.assert_called_once_with(bag_dir)
    mock_create.assert_called_once_with(bag_dir, packager.rights_ids)
    mock_deliver_derivatives.assert_called_once_with()
    mock_poster.assert_called_once_with(bag_dir)
    mock_download.assert_called_once_with(bag_dir)


@patch('package.Packager.download_files')
@patch('package.Packager.cleanup_failed_job')
@patch('package.Packager.deliver_failure_notification')
def test_run_with_exception(mock_notification, mock_cleanup, mock_download):
    packager = Packager(*DEFAULT_ARGS)
    exception = Exception("Error downloading bag.")
    mock_download.side_effect = exception
    packager.run()
    mock_cleanup.assert_called_once_with(
        Path(packager.tmp_dir, packager.refid))
    mock_notification.assert_called_once_with(exception)


@mock_s3
def test_download_files():
    """Asserts files are downloaded correctly."""
    packager = Packager(*DEFAULT_ARGS)
    bucket_name = packager.source_bucket
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=bucket_name)
    expected_len = len(list(Path().glob(f"fixtures/{packager.refid}/*")))
    for obj_path in Path().glob(f"fixtures/{packager.refid}/*"):
        s3.put_object(
            Bucket=bucket_name,
            Key=f"{packager.refid}/{obj_path.name}",
            Body='')

    packager.download_files(Path(packager.tmp_dir, packager.refid))
    tmp_files = len(list(Path(packager.tmp_dir, packager.refid).glob('*')))
    assert tmp_files == expected_len
    for p in [
            Path(packager.tmp_dir, packager.refid, f"{packager.refid}_ma.wav"),
            Path(packager.tmp_dir, packager.refid, f"{packager.refid}_a.mp3")]:
        assert p.is_file()


def test_create_poster():
    """Asserts poster image is created as expected."""
    packager = Packager(*VIDEO_ARGS)
    fixture_path = Path('fixtures', packager.refid)
    tmp_path = Path(packager.tmp_dir, packager.refid)
    copytree(fixture_path, tmp_path)

    packager.create_poster(tmp_path)
    assert Path(tmp_path, "poster.png").is_file()


def test_derivative_map_audio():
    """Asserts information for audio derivatives is correctly produced."""
    packager = Packager(*DEFAULT_ARGS)
    bag_dir = Path(packager.tmp_dir, packager.refid)
    map = packager.derivative_map()
    assert len(map) == 1
    assert map == [
        (bag_dir / f"{packager.refid}_a.mp3",
         packager.destination_bucket_audio_access,
         'audio/mpeg')]


def test_derivative_map_video():
    """Asserts information for video derivatives is correctly produced."""
    packager = Packager(*VIDEO_ARGS)
    bag_dir = Path(packager.tmp_dir, packager.refid)
    map = packager.derivative_map()
    assert len(map) == 3
    assert map == [
        (bag_dir / f"{packager.refid}_me.mov",
            packager.destination_bucket_video_mezzanine, "video/quicktime"),
        (bag_dir / f"{packager.refid}_a.mp4",
            packager.destination_bucket_video_access, "video/mp4"),
        (bag_dir / "poster.png",
            packager.destination_bucket_poster, "image/x-png")]


@mock_s3
def test_deliver_derivatives():
    """Assert derivatives are delivered to correct buckets and deleted locally."""
    packager = Packager(*VIDEO_ARGS)
    fixture_path = Path('fixtures', packager.refid)
    tmp_path = Path(packager.tmp_dir, packager.refid)
    copytree(fixture_path, tmp_path)
    poster = tmp_path / "poster.png"
    poster.touch()

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=packager.destination_bucket_video_access)
    s3.create_bucket(Bucket=packager.destination_bucket_video_mezzanine)
    s3.create_bucket(Bucket=packager.destination_bucket_poster)

    packager.deliver_derivatives()

    assert s3.get_object(
        Bucket=packager.destination_bucket_video_access,
        Key=f"{packager.refid}.mp4")
    assert s3.get_object(
        Bucket=packager.destination_bucket_video_mezzanine,
        Key=f"{packager.refid}.mov")
    assert s3.get_object(
        Bucket=packager.destination_bucket_poster,
        Key=f"{packager.refid}.png")
    assert Path(tmp_path, f"{packager.refid}_ma.mov").is_file()
    assert not Path(tmp_path, f"{packager.refid}_me.mov").is_file()
    assert not Path(tmp_path, f"{packager.refid}_a.mp4").is_file()
    assert not Path(tmp_path, "poster.png").is_file()


def test_create_bag():
    """Asserts bag is created as expected."""
    packager = Packager(*DEFAULT_ARGS)
    fixture_path = Path('fixtures', packager.refid)
    tmp_path = Path(packager.tmp_dir, packager.refid)
    copytree(fixture_path, tmp_path)

    packager.create_bag(tmp_path, packager.rights_ids)
    bag = bagit.Bag(str(tmp_path))
    assert bag.is_valid()
    # TODO assert bag-info


def test_compress_bag():
    """Asserts compressed files are correctly created and original directory is removed."""
    packager = Packager(*DEFAULT_ARGS)
    fixture_path = Path('fixtures', packager.refid)
    tmp_path = Path(packager.tmp_dir, packager.refid)
    copytree(fixture_path, tmp_path)
    bagit.make_bag(tmp_path)

    compressed = packager.compress_bag(tmp_path)
    assert compressed.is_file()
    assert not tmp_path.exists()


@mock_s3
def test_deliver_package():
    """Asserts compressed package is delivered and local copy is removed."""
    packager = Packager(*DEFAULT_ARGS)
    compressed_file = f"{packager.refid}.tar.gz"
    fixture_path = Path('fixtures', compressed_file)
    tmp_path = Path(packager.tmp_dir, compressed_file)
    copyfile(fixture_path, tmp_path)
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=packager.destination_bucket)

    packager.deliver_package(tmp_path)
    assert s3.get_object(
        Bucket=packager.destination_bucket,
        Key=compressed_file)
    assert not tmp_path.exists()


@mock_s3
def test_cleanup_successful_job():
    """Asserts successful job is cleaned up as expected."""
    packager = Packager(*DEFAULT_ARGS)
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=packager.source_bucket)
    s3.put_object(
        Bucket=packager.source_bucket,
        Key=f"{packager.refid}/foo",
        Body='')
    s3.put_object(
        Bucket=packager.source_bucket,
        Key=f"{packager.refid}/bar",
        Body='')

    packager.cleanup_successful_job()

    deleted = s3.list_objects(
        Bucket=packager.source_bucket,
        Prefix=packager.refid).get('Contents', [])
    assert len(deleted) == 0


def test_cleanup_failed_job():
    """Asserts failed job is cleaned up as expected."""
    packager = Packager(*DEFAULT_ARGS)
    fixture_path = Path("fixtures", "b90862f3baceaae3b7418c78f9d50d52")
    compressed_fixture_path = Path("fixtures",
                                   "b90862f3baceaae3b7418c78f9d50d52.tar.gz")
    tmp_path = Path(packager.tmp_dir, packager.refid)
    compressed_tmp_path = Path(packager.tmp_dir,
                               "b90862f3baceaae3b7418c78f9d50d52.tar.gz")
    copytree(fixture_path, tmp_path)
    copyfile(compressed_fixture_path, compressed_tmp_path)

    packager.cleanup_failed_job(tmp_path)

    assert not tmp_path.is_dir()
    assert not compressed_tmp_path.is_file()


@mock_sns
@mock_sqs
def test_deliver_success_notification():
    """Assert success notifications are delivered as expected."""
    sns = boto3.client('sns', region_name='us-east-1')
    topic_arn = sns.create_topic(Name='my-topic')['TopicArn']
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    sqs_conn.create_queue(QueueName="test-queue")
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:test-queue",
    )

    default_args = DEFAULT_ARGS
    default_args[-1] = topic_arn
    packager = Packager(*default_args)

    packager.deliver_success_notification()

    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['MessageAttributes']['format']['Value'] == packager.format
    assert message_body['MessageAttributes']['outcome']['Value'] == 'SUCCESS'
    assert message_body['MessageAttributes']['refid']['Value'] == packager.refid


@mock_sns
@mock_sqs
def test_deliver_failure_notification():
    """Asserts failure notifications are delivered as expected."""
    sns = boto3.client('sns', region_name='us-east-1')
    topic_arn = sns.create_topic(Name='my-topic')['TopicArn']
    sqs_conn = boto3.resource("sqs", region_name="us-east-1")
    sqs_conn.create_queue(QueueName="test-queue")
    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=f"arn:aws:sqs:us-east-1:{DEFAULT_ACCOUNT_ID}:test-queue",
    )

    default_args = DEFAULT_ARGS
    default_args[-1] = topic_arn
    packager = Packager(*default_args)
    exception_message = "foo"
    exception = Exception(exception_message)

    packager.deliver_failure_notification(exception)

    queue = sqs_conn.get_queue_by_name(QueueName="test-queue")
    messages = queue.receive_messages(MaxNumberOfMessages=1)
    message_body = json.loads(messages[0].body)
    assert message_body['MessageAttributes']['format']['Value'] == packager.format
    assert message_body['MessageAttributes']['outcome']['Value'] == 'FAILURE'
    assert message_body['MessageAttributes']['refid']['Value'] == packager.refid
    assert message_body['MessageAttributes']['message']['Value'] == exception_message
