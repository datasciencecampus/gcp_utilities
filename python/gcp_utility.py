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
 - read data from BigQuery
 - ingest files or dataframes into BigQuery 
 - run a Scheduled Query in BigQuery
 - delete a collection in FireStore
 - update a document in a Collection in FireStore

"""

from google.cloud import storage
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud import firestore
import json
import time


#############################################################################
######## GCS Functions

def extract_from_uri(uri):
    """ split a ur into a bucket_id and object_id
    Args:
       url (str): uri reference to a file in GCS
    
    Returns:
        bucket_id, object_id
    """
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
        bucket_name (str): name of bucket, e.g., 'bucket-name'
        destination_blob_name (str): name of the file once uploaded, e.g., 'subfolder/filename.csv'
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

           
#############################################################################
######## Pub/Sub Functions

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

      
#############################################################################
######## BigQuery Functions

def read_data_from_bigquery_to_df(sql):
    """
    Gets data from BigQuery and saves to Pandas DataFrame 
 
    Args:
       sql (str): the sql query to determine what data to return
    Returns:
       the query results in a Pandas dataframe , or None if error
    """
    try:
        client = bigquery.Client()
        df = client.query(sql).to_dataframe()
        return df
    except Exception as e:
        print(f"Error getting data {e}")
        return None 
 

def ingest_dataframe_to_bigquery(project_id, dataset_id, table_id, write_type, schema, skip_rows):
    """
       [
          bigquery.SchemaField("column1", bigquery.enums.SqlTypeNames.DATE),
          bigquery.SchemaField("column2", bigquery.enums.SqlTypeNames.STRING),
          bigquery.SchemaField("column3", bigquery.enums.SqlTypeNames.INTEGER),
          bigquery.SchemaField("column4", bigquery.enums.SqlTypeNames.FLOAT)
        ]
    
    """
""" Save the league table to Big Query"""
    client = bigquery.Client()
    table_id = 'ons-hotspot-prod.processing.incidence_league_tables'
    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=schema,
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data, creating it if it doesnt exist.
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

def save_league_table_date(df,dataset_id, table_id, write_type, schema):
    """ Save dataframe to BigQuery Table
  
    Args:
        df (dataframe): the dataframe with the data to ingest
        dataset_id (str): name of the dataset where data will be stored
        table_id (str): name of the table where data will be ingested
        schema (bigquery.schema): the schema of the data
  
        see https://cloud.google.com/bigquery/docs/schemas
        update_type (bigquery.WriteDisposition): the method of table update, defaults to WRITE_TRUNCATE
        see https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition
    
    Example Schema:
    See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
    
            # Specify the type of columns whose type cannot be auto-detected. For
            # example text columns uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("Col1", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("Col2", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("Col3", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("Col4", bigquery.enums.SqlTypeNames.FLOAT),
    """
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset_id}.{table_id}'
    job_config = bigquery.LoadJobConfig(schema=schema)
    
    # Set the update type
    if write_type == 'overwrite':
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    
    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  # Make an API request.
    
    job.result()  # Wait for the job to complete.
    
    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def ingest_csv_to_bigquery(uri, dataset_id, table_id, write_type, schema, skip_rows):
    """
    Ingests a csv file at location uri into BigQuery

    Args:
        uri (str): the uri of the file to ingest 
        dataset_id (str): name of the dataset where data will be stored
        table_id (str): name of the table where data will be ingested
        schema (bigquery.schema): the schema of the data
        skip_rows (int): number of rows to skip
        see https://cloud.google.com/bigquery/docs/schemas
        update_type (bigquery.WriteDisposition): the method of table update, defaults to WRITE_TRUNCATE
        see https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/WriteDisposition
    """
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    
    # Set the update type
    if write_type == 'overwrite':
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
         
    # Define Schema - do not add if no schema defined
    if len(schema) == 0:
        job_config.autodetect = True
    else:
        job_config.schema = schema

    job_config.skip_leading_rows = skip_rows

    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    
    print('Starting Ingest into BQ')
    print(schema)
    try:
        print(f"{uri} {table_id} {datsaet_ref}")
        load_job = client.load_table_from_uri(
            uri, dataset_ref.table(table_id), job_config=job_config
        ) 
        load_job.result()  # Waits for table load to complete.  
        print("Ingest completed.")
        destination_table = client.get_table(dataset_ref.table(table_id))

        print(f"Loaded {destination_table.num_rows} rows from {uri}.")
        return destination_table
    except Exception as e:
        print(f"Error ingesting data into BigQuery {e}")
        return None


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
        print(f"Error running schedule query {e}")
        return False

#############################################################################
######## Firestore Functions

def delete_collection(collection_name, batch_size=200):
    """ Delete a given collection in FireStore
    
    Args:
      collection_name (str): the name of the collection
      batch_size (int): the delete batch size
    
    """
    try:
        db = firestore.Client()
        coll_ref = db.collection(collection_name)
        docs = coll_ref.limit(batch_size).stream()
        # Keep a record of how many docs are deletec
        deleted = 0
        for doc in docs:
            doc.reference.delete()
            deleted = deleted + 1
        print(f"Deleted {deleted} docs")
        if deleted >= batch_size:
            return delete_collection(collection_name, batch_size)
    except Exception as e:
        print(f"Error Deleting Collection docs {e}")
      

def update_firestore_document(collection, document, values_dict):
    """ Updates a document in a collection in FireStore
    
    values should be passed as a dict e.g.:
       {
            u'key1': parameter,
            u'key2': u'value2',
            u'datetime': datetime.datetime.now()
        }
    
    Args:
      collection (str): 
      document (str):
      values_dict (dict):
    """
    try:
        db = firestore.Client()
        doc_ref = db.collection(collection).document(document)
        doc_ref.set(values_dict)
        return
    except Exception as e:
        print(f"Error updating State {e}")
        return

def check_firestore_values(collection, query):
    """ Query the documents in a given collection
    
    Example query:
      query = ["key1", "==", "value"]
      query = ["key2", ">", 34)
      query = 'all'
      You can build and explore what query to use within the FireStore GUI
    Args:
      collection (str): the collection to query
      query (list of str): the query statement, if 'all' then 
      all documents will be returned
    Returns:
      a list of docs, or None if error

    """
    try:
        db = firestore.Client() 
        coll_ref = db.collection(collection)
        # Run Query
        if query != 'all':
            results = coll_ref.where(query[0], query[1],query[2]).stream()  
        else:
            results = coll_ref.stream()
        return results
    except Exception as e:
        print(f"Error getting State {e}")
        return False
