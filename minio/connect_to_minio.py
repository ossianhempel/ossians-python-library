import os
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv

def connect_to_minio(endpoint, access_key, secret_key):
    try:
        client = Minio(endpoint,
                        access_key=access_key, # user id
                        secret_key=secret_key, # service password
                        secure=False, # TODO - make these work with true
                        cert_check=False,
                    )
        
        
        print('Connected to MinIO')
        return client
    
    except S3Error as e:
        print("S3 Error: ", e)
        return None