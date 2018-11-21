__author__ = 'Tejas Bichave'
__copyright__ = 'Copyright 2018, Searce'
__version__ = '1.0.0'
__maintainer__ = 'Tejas Bichave'
__email__ = 'tejas.bichave@searce.com'
__status__ = 'Development'


""" -- Python Libraries -- """
import os
from google.cloud import storage
""" -- Django Libraries -- """

""" --  Project Libraries -- """
from settings import GOOGLE_APPLICATION_CREDENTIALS

class GcsBucketName(object):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
    def __init__(self, bucket_name):
        self.__bucket_name = bucket_name

    def get_bucket_name(self):
        return self.__bucket_name

    def set_bucket_name(self, bucket_name):
        self.__bucket_name = bucket_name

class BlobOperations(object):
    def download_blob(self, soucre_blob_name, destination_file_name, gcs_name, bucket_name):
        """Download a file from a bucket."""
        bucket = BucketOperations().initialize_bucket(gcs_name, bucket_name)
        blob = bucket.blob(soucre_blob_name)
        blob.download_to_filename(destination_file_name)
        print('File {} downloaded at {}.'.format(
            soucre_blob_name,
            destination_file_name))

    def upload_blob(self,bucket_name, source_file_name, destination_blob_name, gcs_name):
        """Uploads a file to the bucket."""
        bucket = BucketOperations().initialize_bucket(gcs_name, bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print('File {} uploaded to {}.'.format(
            source_file_name,
            destination_blob_name))

    def list_blobs(self, gcs_name,bucket_name):
        """Lists all the blobs in the bucket."""
        bucket = BucketOperations().initialize_bucket(gcs_name, bucket_name)
        blobs = bucket.list_blobs()
        for blob in blobs:
            print(blob.name)

    def rename_blob(self,bucket_name, blob_name, new_name,gcs_name):
        """Renames a blob."""
        bucket = BucketOperations().initialize_bucket(gcs_name, bucket_name)
        blob = bucket.blob(blob_name)
        new_blob = bucket.rename_blob(blob, new_name)

        print('Blob {} has been renamed to {}'.format(
            blob.name, new_blob.name))

    def copy_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):
        """Copies a blob from one bucket to another with a new name."""
        storage_client = storage.Client()
        source_bucket = storage_client.get_bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.get_bucket(new_bucket_name)

        new_blob = source_bucket.copy_blob(
            source_blob, destination_bucket, new_blob_name)

        print('Blob {} in bucket {} copied to blob {} in bucket {}.'.format(
            source_blob.name, source_bucket.name, new_blob.name,
            destination_bucket.name))

    def blob_metadata(self,bucket_name, blob_name,gcs_name):
        """Prints out a blob's metadata."""
        bucket = BucketOperations().initialize_bucket(gcs_name, bucket_name)
        blob = bucket.get_blob(blob_name)

        print('Blob: {}'.format(blob.name))
        print('Bucket: {}'.format(blob.bucket.name))
        print('Storage class: {}'.format(blob.storage_class))
        print('ID: {}'.format(blob.id))
        print('Size: {} bytes'.format(blob.size))
        print('Updated: {}'.format(blob.updated))
        print('Generation: {}'.format(blob.generation))
        print('Metageneration: {}'.format(blob.metageneration))
        print('Etag: {}'.format(blob.etag))
        print('Owner: {}'.format(blob.owner))
        print('Component count: {}'.format(blob.component_count))
        print('Crc32c: {}'.format(blob.crc32c))
        print('md5_hash: {}'.format(blob.md5_hash))
        print('Cache-control: {}'.format(blob.cache_control))
        print('Content-type: {}'.format(blob.content_type))
        print('Content-disposition: {}'.format(blob.content_disposition))
        print('Content-encoding: {}'.format(blob.content_encoding))
        print('Content-language: {}'.format(blob.content_language))
        print('Metadata: {}'.format(blob.metadata))

class BucketOperations(object):

    def create_bucket(self,bucket_name):
        """Creates a new bucket."""
        storage_client = storage.Client()
        bucket = storage_client.create_bucket(bucket_name)
        print('Bucket {} created'.format(bucket.name))

    def bucket_exists(self,bucket_name):
        is_bucket=False
        try:
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(bucket_name)
            is_bucket = True

        except Exception :
            print('Sorry, that bucket does not exist!')
        return is_bucket

    def initialize_bucket(self, gcs_name, bucket_name):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
        storage_client = storage.Client(project=gcs_name)
        return storage_client.get_bucket(bucket_name)