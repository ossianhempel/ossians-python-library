import os
from minio import Minio
from minio.error import S3Error
import pandas as pd

def fetch_object_as_dataframe(client: Minio, bucket_name: str, object_name: str) -> pd.DataFrame:
    """
    fetch object from minio and return as pandas dataframe
    """
    try:
        response = client.get_object(bucket_name, object_name)
        data = response.read()
        response.release_conn()
        print(f"Fetched '{object_name}' from bucket '{bucket_name}'")

        # Convert bytes data to a pandas DataFrame
        data_stream = io.BytesIO(data)
        df = pd.read_csv(data_stream)
        return df
    except S3Error as e:
        print("S3 Error: ", e)
        return None
    except Exception as e:
        print("Error: ", e)
        return None