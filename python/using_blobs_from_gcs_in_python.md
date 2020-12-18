# How to access and use blobs from Google Cloud Storage (GCS) in Python

Accessing blob objects from Google Cloud Storage (GCS) is straightforward using the [Python Client for Google Cloud Storage](https://googleapis.dev/python/storage/latest/index.html) - but how do you then parse the blob to a usable Python Object that you are faimilar with? Hopefully the following examples will help.

## Accessing Blobs
Lets say you have a blob object called ```folder1/file.csv``` in a bucket ```my-bucket``` then getting a reference to the blob can be achieved using ```def get_file_blob_from_gcs("my-bucket", "folder1/file.csv")```, where:

```
from google.cloud import storage

def get_file_blob_from_gcs(bucket_name, blob_name):
    """ Gets a blob from a file in a given bucket
    Once the blob is obtained you can download as appropriate
     
    Args:
      bucket_name (str): the name of the bucket where the file sits 
      blob_name (str): the object name of the blob 
    Return:
      returns the requested blob, or None if there was an error
    
    """
    try:
        storage_client = storage.Client()
        # Get the blob from the bucket
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob
    except Exception as e:
        print("Error getting file {}/{}, Error Message {}".format(bucket_name, blob_name, e))
        return None
```

Any 'folders' that the object sits in are part of its name.

## Downloading Blobs
Now you have a reference to the blob - how do you use it? You can download it one of two ways:

1. Download as Bytes using ```blod.download_as_bytes() ```
2. Download as Text using ```blob.download_as_text()```

The now depreciated ```download_as_string()``` function downloaded the file contents as bytes. Depending on the version you are using this may be what you need to use. 

At this point we can feed the downloaded contents into Python, the trick is knowing what format the Python object is expecting and formating the download appropriately. For example, onjects usuelly used to reading from files will expect a bytes string. In this case we can use the base Python ```io``` package to help. 

## Loading data files
### Loading JSON blobs into Python Dictionaries
Use the built in [JSON](https://docs.python.org/3/library/json.html) support and import in as bytes. 
```
import json

blob = get_file_blob_from_gcs(bucket_name, blob_name)
python_dict = json.loads(blob.download_as_bytes())
```

### Loading CSV blobs into Pandas Dataframes
Pandas expects a stream of text from the file, so use ```download_as_text()``` as use StringIO to stream this in.
```
import io
import pandas as pd
     
blob = get_file_blob_from_gcs(bucket_name, blob_name)
csv_contents = blob.download_as_text()
df = pd.read_csv(io.StringIO(csv_contents))
```

If we expected a bytes stream, we would use ```io.BytesIO(blob.download_as_bytes())```

## Loading geography files
### Loading GeoJSON objects 

#### GeoJSON for use in Folium
This is the same as loading a JSON file.
```
import json
import folium

geojson_data = json.loads(blob.download_as_bytes())

# Create a Folium Map and add the GeoJSON layer
point_of_interest_coords = [51.4816, -3.1791]
geo_map = folium.Map(location=point_of_interest_coords,
                     zoom_start=12,
                     tiles='Stamen Toner')
folium.GeoJson(
    geojson_data,
    name='Example geoJSON'
).add_to(geo_map)

folium.LayerControl().add_to(geo_map)
# Display the map
geo_map
```

### GeoJSON for use in GeoPandas
```
import geopandas

blob = get_file_blob_from_gcs(bucket_name, blob_name)
gdf = geopandas.read_file(blob.download_as_string().decode('utf-8'))
gdf.plot()
```

### Loading shapefiles
You can load shapefiles directly into Geopands using the full object uri of the .shp file, but ensure that the associated shapeile information files are in the same location:
```
import geopandas

shapefile = geopandas.read_file("gs://my-bucket/folder1/example_sampefile.shp", driver='ESRI Shapefile')
shapefile.plot()
```

