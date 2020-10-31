# Google Cloud Platform (GCP) Utilities and Example Code
Useful generic code to interact with Cloud Native tools on Google Cloud Platform.

## Python 
The code in [gcp_utility.py](/blob/main/python/gcp_utility.py) includes functions that could be used in Cloud Functions and Cloud Run to interact with

- [BigQuery](https://cloud.google.com/bigquery) 
- [Google Cloud Storage](https://cloud.google.com/storage)
- [Pub/Sub](https://cloud.google.com/pubsub)
- [FireStore](https://cloud.google.com/firestore)

This Python code builds on the https://github.com/googleapis/google-api-python-client library, but wraps some common functions for easier error handling and logging to StackDriver. This can make developing and working wih Cloud Functions much easier. Examples of using these can be found in the [cloud functions](blob/main/python/cloud_functions) example
