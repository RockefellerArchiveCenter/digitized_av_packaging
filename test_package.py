from pathlib import Path
from shutil import copyfile, copytree, rmtree
from unittest.mock import patch

import bagit
import boto3
import pytest
from moto import mock_s3

from package import Packager

DEFAULT_ARGS = ['audio', 'b90862f3baceaae3b7418c78f9d50d52', ["1", "2"], "tmp", "source", "destination",
                "destination_mi_mezz", "destination_mi_access", "destination_audio_access", "destination_poster"]
MOVING_IMAGE_ARGS = ['moving image', '20f8da26e268418ead4aa2365f816a08', ["1", "2"], "tmp", "source", "destination",
                     "destination_mi_mezz", "destination_mi_access", "destination_audio_access", "destination-poster"]


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

    invalid_args = [None, 'b90862f3baceaae3b7418c78f9d50d52', ["1", "2"], "tmp", "source", "destination",
                    "destination_mezz", "destination_access", "destination_poster"]
    with pytest.raises(Exception):
        Packager(*invalid_args)


@patch('package.Packager.download_files')
@patch('package.Packager.create_poster')
@patch('package.Packager.deliver_derivatives')
@patch('package.Packager.create_bag')
@patch('package.Packager.compress_bag')
@patch('package.Packager.deliver_package')
def test_run(mock_deliver, mock_compress, mock_create,
             mock_deliver_derivatives, mock_poster, mock_download):
    """Asserts run method calls other methods."""
    Packager(*DEFAULT_ARGS).run()
    assert mock_deliver.called_once_with()
    assert mock_compress.called_once_with()
    assert mock_create.called_once_with()
    assert mock_deliver_derivatives.called_once_with()
    assert mock_poster.called_once_with()
    assert mock_download.called_once_with()


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
    assert len(
        s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=packager.refid).get(
            'Contents',
            [])) == 0


def test_create_poster():
    """Asserts poster image is created as expected."""
    packager = Packager(*MOVING_IMAGE_ARGS)
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


def test_derivative_map_moving_image():
    """Asserts information for moving image derivatives is correctly produced."""
    packager = Packager(*MOVING_IMAGE_ARGS)
    bag_dir = Path(packager.tmp_dir, packager.refid)
    map = packager.derivative_map()
    assert len(map) == 3
    assert map == [
        (bag_dir / f"{packager.refid}_me.mov",
            packager.destination_bucket_moving_image_mezzanine, "video/quicktime"),
        (bag_dir / f"{packager.refid}_a.mp4",
            packager.destination_bucket_moving_image_access, "video/mp4"),
        (bag_dir / "poster.png",
            packager.destination_bucket_poster, "image/x-png")]


@mock_s3
def test_deliver_derivatives():
    """Assert derivatives are delivered to correct buckets and deleted locally."""
    packager = Packager(*MOVING_IMAGE_ARGS)
    fixture_path = Path('fixtures', packager.refid)
    tmp_path = Path(packager.tmp_dir, packager.refid)
    copytree(fixture_path, tmp_path)
    poster = tmp_path / "poster.png"
    poster.touch()

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket=packager.destination_bucket_moving_image_access)
    s3.create_bucket(Bucket=packager.destination_bucket_moving_image_mezzanine)
    s3.create_bucket(Bucket=packager.destination_bucket_poster)

    packager.deliver_derivatives()

    assert s3.get_object(
        Bucket=packager.destination_bucket_moving_image_access,
        Key=f"{packager.refid}.mp4")
    assert s3.get_object(
        Bucket=packager.destination_bucket_moving_image_mezzanine,
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
