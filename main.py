import boto3
from dotenv import load_dotenv
import os
from typing import List

# Load environment variables from .env file
load_dotenv()

# AWS Configurations from .env
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

# Configure S3 Client
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    print("S3 client configured successfully.")
except Exception as e:
    print(f"Error configuring S3 client: {e}")


def list_files(folder: str) -> List[str]:
    files = []
    try:
        for file_name in os.listdir(folder):
            path_complete = os.path.join(folder, file_name)
            if os.path.isfile(path_complete):
                files.append(path_complete)
        print(f"Files found: {files}")
    except Exception as e:
        print(f"Error listing files in folder '{folder}': {e}")
        raise
    return files

def upload_files_to_s3(files: List[str]) -> None:
    for file in files:
        file_name = os.path.basename(file)
        try:
            print(f"Attempting to upload '{file_name}' to bucket '{BUCKET_NAME}'...")
            s3_client.upload_file(file, BUCKET_NAME, file_name)
            print(f"'{file_name}' successfully uploaded to S3.")
        except Exception as e:
            print(f"Error uploading '{file_name}' to S3: {e}")


def delete_local_files(files: List[str]) -> None:
    for file in files:
        try:
            os.remove(file)
            print(f"'{file}' was deleted from the local folder.")
        except Exception as e:
            print(f"Error deleting file '{file}': {e}")
            
def execute_backup(folder: str) -> None:
    try:
        print(f"Iniciando o processo de backup para a pasta '{folder}'...")
        files: List[str] = list_files(folder)
        if files:
            upload_files_to_s3(files)
            delete_local_files(files)
        else:
            print("No files found for backup.")
    except Exception as e:
        print(f"Error processing backup: {e}")
        raise



if __name__ == '__main__':
    LOCAL_FOLDER: str = 'download'
    try:
        execute_backup(LOCAL_FOLDER)
    except Exception as e:
        print(f"Error executing backup: {e}")
