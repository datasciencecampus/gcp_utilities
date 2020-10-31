"""
allows streaming pf large files to GC Storage
Useful for unzipping or encrypting large files

This code is taken from

https://dev.to/sethmlarson/python-data-streaming-to-google-cloud-storage-with-resumable-uploads-458h

Eample use in Cloud function:

    import base64
    import json
    from google.cloud import storage

    import gcs_streaming


    def hello_pubsub(event, context):
         """Triggered from a message on a Cloud Pub/Sub topic.
         Streams example data to a file in GCS 

         Args:
             event (dict): Event payload.
             context (google.cloud.functions.Context): Metadata for the event.

         Pubsub message should be of the form of:
            {"bucketId":"bucket-id","objectId":"test-blob-namne"}
         """
         pubsub_message = base64.b64decode(event['data']).decode('utf-8')
         json_msg = json.loads(pubsub_message)
         try:
              bucket_id = json_msg['bucketId']
              object_id = json_msg['objectId']
         except Exception as e:
              print(e)

         client = storage.Client()
         try:
              with gcs_streaming.GCSObjectStreamUpload(client=client, bucket_name=bucket_id, blob_name=object_id) as s:
                   for _ in range(1024):
                        s.write(b'test' * 1024)
         except Exception as e:
              print(e)


requirements.txt:
  google-resumable-media
  google-cloud-storage
  google-auth

"""

from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import requests, common
from google.cloud import storage

class GCSObjectStreamUpload(object):
    def __init__(
            self, 
            client: storage.Client,
            bucket_name: str,
            blob_name: str,
            chunk_size: int=256 * 1024
        ):
        self._client = client
        self._bucket = self._client.bucket(bucket_name)
        self._blob = self._bucket.blob(blob_name)

        self._buffer = b''
        self._buffer_size = 0
        self._chunk_size = chunk_size
        self._read = 0

        self._transport = AuthorizedSession(
            credentials=self._client._credentials
        )
        self._request = None  # type: requests.ResumableUpload

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.stop()

    def start(self):
        url = (
            f'https://www.googleapis.com/upload/storage/v1/b/'
            f'{self._bucket.name}/o?uploadType=resumable'
        )
        self._request = requests.ResumableUpload(
            upload_url=url, chunk_size=self._chunk_size
        )
        self._request.initiate(
            transport=self._transport,
            content_type='application/octet-stream',
            stream=self,
            stream_final=False,
            metadata={'name': self._blob.name},
        )

    def stop(self):
        self._request.transmit_next_chunk(self._transport)

    def write(self, data: bytes) -> int:
        data_len = len(data)
        self._buffer_size += data_len
        self._buffer += data
        del data
        while self._buffer_size >= self._chunk_size:
            try:
                self._request.transmit_next_chunk(self._transport)
            except common.InvalidResponse:
                self._request.recover(self._transport)
        return data_len

    def read(self, chunk_size: int) -> bytes:
        # I'm not good with efficient no-copy buffering so if this is
        # wrong or there's a better way to do this let me know! :-)
        to_read = min(chunk_size, self._buffer_size)
        memview = memoryview(self._buffer)
        self._buffer = memview[to_read:].tobytes()
        self._read += to_read
        self._buffer_size -= to_read
        return memview[:to_read].tobytes()

    def tell(self) -> int:
        return self._read
