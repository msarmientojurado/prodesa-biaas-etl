
from libraries.settings import BLOB_NAME_VALIDATION_REPORT, BUCKET_NAME_VALIDATION_REPORT

from google.cloud import storage
import io


def store_process_result(output):

    output.seek(0)
    #Storing the result to the bucket
    # bucket name
    bucket = BUCKET_NAME_VALIDATION_REPORT

    # Get the bucket that the file will be uploaded to.
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)

    # Create a new blob and upload the file's content.
    my_file = bucket.blob(BLOB_NAME_VALIDATION_REPORT)
    
    # upload from string
    my_file.upload_from_string(output.read(), content_type="text/plain")

    output.close()
    return