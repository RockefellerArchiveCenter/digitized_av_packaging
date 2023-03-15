import os


class Packager(object):

    def __init__(self, format, refid):
        self.format = format
        self.refid = refid

    def run(self):
        """Main method, which calls all other methods."""
        delivered_files = self.discover_files(self.refid)
        poster_image = self.create_poster(self.refid)
        self.deliver_derivatives(delivered_files + poster_image)
        self.create_bag(self.refid)
        self.add_rights_ids(self.rights_ids)
        self.compress_bag(self.refid)
        self.deliver_package(self.refid)

    def discover_files(self, bag_dir):
        """Lists all files in a directory."""
        pass

    def create_poster(self, bag_dir):
        """Creates a poster image from a video file."""
        pass

    def deliver_derivatives(self, file_list):
        """Uploads derivatives to S3 buckets and deletes them from temporary storage."""
        pass

    def create_bag(self, bag_dir):
        """Creates a BagIt bag from a directory."""
        pass

    def add_rights_ids(self, rights_ids):
        """Adds rights identifiers to bag-info.txt."""
        pass

    def compress_bag(self, bag_dir):
        """Creates a compressed archive file from a bag."""
        pass

    def deliver_package(self, bag_dir):
        """Delivers packaged files to destination."""
        pass


if __name__ == '__main__':
    format = os.environ.get('FORMAT')
    refid = os.environ.get('REFID')
    Packager(format, refid).run()
