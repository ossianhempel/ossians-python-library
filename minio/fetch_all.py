import os
from minio import Minio
from minio.error import S3Error
import pandas as pd

def fetch_files(client, bucket_name, prefix):
    if client is None:
        print("Failed to connect to MinIO")
        return None

    files = {}
    try:
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        for obj in objects:
            response = client.get_object(bucket_name, obj.object_name)
            data = response.read()
            response.release_conn()
            files[obj.object_name] = data
            print(f"Fetched '{obj.object_name}' from bucket '{bucket_name}'")
    except S3Error as e:
        print("S3 Error: ", e)
        return None
    except Exception as e:
        print("Error: ", e)
        return None