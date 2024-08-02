import os
from minio import Minio
from minio.error import S3Error
import pandas as pd


def upload_files(files: dict, local_directory: str, prefix_to_remove: str = '') -> None:
    """
    upload files to local directory
    """
    if not files:
        print("No files to upload")
        return

    try:
        for file_name, data in files.items():
            local_file_path = os.path.join(local_directory, file_name.replace(prefix_to_remove, ''))
            create_local_directory(local_file_path)
            save_file_to_local(data, local_file_path)
            print(f"Uploaded '{file_name}' to '{local_file_path}'")
    except Exception as e:
        print("Error: ", e)