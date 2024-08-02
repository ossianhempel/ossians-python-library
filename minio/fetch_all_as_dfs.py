import os
from minio import Minio
from minio.error import S3Error
import pandas as pd

def fetch_all_objects_as_dataframes(client: Minio, bucket_name: str) -> dict:
    """
    fetch all objects from minio bucket and return as a dictionary of pandas dataframes
    """
    dataframes = {}

    try:
        objects = client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            df = fetch_object_as_dataframe(client, bucket_name, obj.object_name)
            if df is not None:
                dataframes[obj.object_name] = df
    except S3Error as e:
        print("S3 Error: ", e)
        return None
    except Exception as e:
        print("Error: ", e)
        return None

    return dataframes