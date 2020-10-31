"""
June 2020
David Pugh, Data Science Campus

Received message from pub/sub, extracts the parameters
get this requested source file from a URL using requests,
and then saves the file to the specified bucket with a 
from given url to specified bucket.

Publishes message to pub/sub topic given in the output_topic_name env variable

Triggered by a Pub/Sub topic, which should have a JSON string as its message
of the following form:

   {
    "source_file_name":"file",
    "bucket_name":"bucket", 
    "destination_blob_name":"destination_file",
    "datediff":6
    }

Requires the following environmnet variables:
    project_id : project_id where pubsub message will be published
    output_topic_name : pubsub topic where message will be published if successful
    error_topic_name : pubsub topic where wmessage will be published if error

The function deals with files that contain a changing date stamp, e.g., a daily data
file that has todats date as part of its title. In this case the date can be substituted
in the source-file_name with pattern $DATEISO. This name patter is observed it is replaced
with the date in ISO format. $DATEDIFF will replace it by a date x days agao, where x is 
given by datediff (supplied in the pub/sub message).The date can also be passed on to the 
destination file name.

"""

import base64
import requests
import os
import json
from time import strftime
from datetime import date, timedelta

# Get functions from the gcp_utility.py module 
from gcp_utility import upload_to_bucket
from gcp_hotspots import message_to_pubsub

def pubsub_trigger(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic, 
    extracts variables from message and uploads file from 

    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    project_id = os.environ.get('PROJECT_ID', 'PROJECT_ID environment variable is not set.')
    error_topic_name = os.environ.get('ERROR_TOPIC_NAME', 'error_topic_name variable is not set.')
    
    # get the pubsub message
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    # recover the variabes from the message
    json_msg = json.loads(pubsub_message)
    try:
        source_file_name = json_msg['source_file_name']
        bucket_name = json_msg['bucket_name']
        destination_blob_name = json_msg['destination_blob_name']
        datediff = json_msg['datediff']
    except Exception as e:
        print(e)
        message_to_pubsub(error_topic_name, "Error getting the variables from Pub/Sub", project_id)
        return   

    # We can deal with URLS with dates that change daily by looking for 
    # $DATE pattern in the source_file_name or destination_blob_name and 
    # then substituding with either todays date or a date 6 days ago
    try:
        if '$DATEISO' in source_file_name:
            source_file_name = replace_iso_date(source_file_name)

        if '$DATEISO' in destination_blob_name:
            destination_blob_name = replace_iso_date(destination_blob_name)

        if '$DATEDIFF' in source_file_name:
            source_file_name = replace_date_diff(source_file_name, datediff)

    except Exception as e:
        print(e)
        message_to_pubsub(error_topic_name, "Error processing date in filename: {} / {}".format(bucket_name, destination_blob_name), project_id)
        return

    print("Downloading...{}".format(source_file_name))
    # get data from url
    response = requests.get(source_file_name, allow_redirects=True)

    # if successful download then upload to bucket
    if response.status_code == 200:
        # Save to bucket
        content_type = response.headers.get('content-type')
        uri = upload_to_bucket(bucket_name, destination_blob_name, response.content, content_type=content_type)
        if uri is None:
            print("Error uploading file to bucket: {} / {}".format(bucket_name, destination_blob_name))
            message_to_pubsub(error_topic_name, "Error uploading file to bucket: {} / {}".format(bucket_name, destination_blob_name), project_id)
            return 
        else:
            print('File {} uploaded to {}'.format(destination_blob_name, bucket_name))

    # if not 200 status then error getting file from url        
    else:
        print("Error getting file {}, code {}".format(source_file_name, response.status_code))
        message_to_pubsub(error_topic_name, "Error downloading file from url: {}".format(source_file_name), project_id)


def replace_iso_date(original_str):
    print("Date detected, placing todays date")
    today = strftime('%Y-%m-%d')
    original_str = original_str.replace('$DATEISO', today)
    return original_str 


def replace_date_diff(original_str, n):
    print(f"DateDiff detected, replacing with date {n} days ago")  
    urldate = date.today() - timedelta(days = n ) 
    original_str = original_str.replace('$DATEDIFF', urldate.strftime('%Y-%m-%d'))
    return original_str
