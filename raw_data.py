## library imports
from google.cloud import storage
import os



##------
## variable defination

GOOGLE_APPLICATION_CREDENTIALS = r'C:\Users\HP\Desktop\data_projects\evaluation\service_key\delta-compass-440906-1551790f643b.json'

SOURCE_BUCKET = 'data-from-source'
CUST_PROFILE_SOURCE = 'cust_profile/customer_profile.csv'
CUST_TRANS_SOURCE = 'cust_tran/customer_transcation.csv'

DESTINATION_BUCKET = 'project-file-raw-layerfiles'
CUST_PROFILE_TARGET = 'cust_profile/'
CUST_TRANS_TARGET ='cust_tran/'
## raw layer

## enviroment variable setting
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS






## utility function to copy sourece bucket to destination bucket
def copy_blob(
    bucket_name, blob_name, destination_bucket_name, destination_blob_name,
):
    """Copies a blob from one bucket to another with a new name."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # destination_bucket_name = "destination-bucket-name"
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to copy is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    # There is also an `if_source_generation_match` parameter, which is not used in this example.
    destination_generation_match_precondition = 0

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name, if_generation_match=destination_generation_match_precondition,
    )

    print(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )



if __name__ == '__main__':

    #  transfer sourcce data to target data

    ## cust_profile transfer object
    cust_profile = copy_blob(SOURCE_BUCKET,CUST_PROFILE_SOURCE,DESTINATION_BUCKET,CUST_PROFILE_SOURCE)

    ## cust_trans transfer object
    cut_trans = copy_blob(SOURCE_BUCKET,CUST_TRANS_SOURCE,DESTINATION_BUCKET,CUST_TRANS_SOURCE)