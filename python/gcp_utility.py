"""

September 2020
David Pugh, Data Science Campus
Please contact datasciencecampus@ons.gov.uk for more information

A set of generic useful functions using the Google Cloud Python API
Wraps common functions in Try Except statements to catch key errors
This is useful when running within Cloud Functions as errors will be 
written to StackDriver logs to help with development.

Interacts with a number of Cloud Native Functions, including:

 - working with uris
 - getting and writing blobs to GC Storage
 - write message to Pub/Sub
 - ingest file into BigQuery
 - run a Scheduled Query in BigQuery
 - delete a collection in FireStore
 - update a document in a Collection in FireStore

"""

from google.cloud import storage
from google.cloud import bigquery
from google.cloud import pubsub_v1
import json
import time
import sys
import pandas as pd

from io import BytesIO
from google.protobuf.timestamp_pb2 import Timestamp

def extract_from_uri(uri):
    uri_s = uri[5:]
    parts = uri_s.split('/',1)
    return parts[0], parts[1]


def get_file_blob(bucket_name, file_name):
    """ Gets a blob from a file in a given bucket
    Once the blob is obtained you can download as appropriate, e.g.,
      blob.download_as_text(client=None)
      blolb.download_as_bytes(client=None)
      
      See https://googleapis.dev/python/storage/latest/blobs.html
    
    Args:
      bucket_name (str): the name of the bucket where the file sits 
      file_name (str): the object name of the blob 
    Return:
      returns the requested blob, or None if there was an error
    
    """
    try:
        storage_client = storage.Client()
        # Get the blob from the bucket
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        return blob
    except Exception as e:
        print("Error getting file {}/{}, Error Message {}".format(bucket_name, file_name, e))
        return None


def get_json_file(bucket_name, file_name):
    """ Gets a JSON file from Google Cloud Storage Bucket
    Args:
      bucket_name (str): the name of the bucket where the file sits
      file_name (str): the object name of the JSON file 
     Returns:
        The contents of the JSON file 
    """
    blob = get_file_blob(bucket_name, file_name)
    if blob is None:
        print("Could not access JSON file, check it is uploaded and you have permission")
        return None    
    # Download the contents of the blob as bytes and then parse it using json.loads() method
    json_file = json.loads(blob.download_as_bytes(client=None))
    if json_file is None:
        print("Could not read JSON file, check it is in the correct format")
        return None
    else:
        print("JSON file obtained")
        return json_file


def upload_to_bucket(bucket_name, destination_blob_name, text, content_type='text/csv'):
    """Uploads the given text to GC Storage as a given file type

    Args:
        bucket_name (str): name of bucket 
        destination_blob_name (str): name of the file once uploaded
        text (str): the data for the file 
        content_type(str): the file type, defaults to text/csv
    Returns:
      The uri of the uploaded blob. Returns None is error.
    """
    # bucket details 
    print('bucket: {}'.format(bucket_name))
    print('destination: {}'.format(destination_blob_name))

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        #bucket = storage_client.get_bucket(bucket_name)
    except Exception as e:
        print("Could not get bucket {}".format(bucket_name))
        print(e)    
        
    else:
        blob = bucket.blob(destination_blob_name)
        # upload to bucket
        try:
            blob.upload_from_string(text, content_type=content_type)
            uri = 'gs://' + bucket_name + '/' + destination_blob_name
            
            print('File {} uploaded to {}'.format(destination_blob_name, bucket_name))
            return uri
        except Exception as e:
            print('Error uploading {} to bucket {}'.format(destination_blob_name, bucket_name))
            print(e)
            return None    


def message_to_pubsub(topic_name, message, project_id):
    """
    Published a message to the Pub/Sub topic in project
    Example use:
        # Send JSON string to Pub/Sub Topic 
            message = {
            "key1": "value1", 
            "key2": "value2
            }
        # Add message to Pub/Sub Queue to launch function
        message_to_pubsub(pubsub_topic_name, json.dumps(message), project_id)
    
    Args:
        topic_name (string) : the pubsub topic name e.g., pubsub-topic
        message (str) : the message to send
        project_id (str): the project id
    """
    try:
        publisher = pubsub_v1.PublisherClient()
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_name}`
        topic_path = publisher.topic_path(project_id, topic_name)


        # Data must be a bytestring
        data = message.encode('utf-8')
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data=data)
        print('Published {} of message ID {}.'.format(data, future.result()))
    except Exception as e:
        print(f"Error with PubSub {e}")


def run_scheduled_query(resource_name):
    """ Runs a scheduled query in BigQuery
    An example resource_name string would be 
    projects/350563100867/locations/europe-west2/transferConfigs/5f3e64eb-0000-2542-998e-3c286d3a6c12
    
    This can be found in the Transfer config details section of the Configuration tab in the 
    Scheduled Query section of BigQuery.
    
      Arg:
        resource_name (string) : the resource name of the scheduled query 
      Returns:
        True if the scheduled query has been instigated successfully, False if there was an error
    """
    print("Running schedule query: {}".format(resource_name))
    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    start_time = bigquery_datatransfer_v1.types.Timestamp(seconds=int(time.time() + 10))
    try:  
        response = client.start_manual_transfer_runs(resource_name, requested_run_time=start_time)
        print("Scheduled Query instigated")
        return True
    except Exception as e:
        print(e)
        return False
