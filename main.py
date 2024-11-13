import boto3
from dotenv import load_dotenv
import os
from typing import List



load_dotenv()

# Configurações da AWS a partir do .env
AWS_ACCESS_KEY_ID: str = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY: str = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION: str = os.getenv('AWS_REGION')
BUCKET_NAME: str = os.getenv('BUCKET_NAME')


s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Configura o cliente S3
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    print("Client configuration succeed.")
except Exception as e:
    print(f"Error configuring client: {e}")
    raise

def list_files(folder:str) -> List[str]:
    files: List[str] = []
    try:
        for file_name in os.listdir(folder):
            path_complet = os.path.join(folder, file_name)
            if os.path.isfile(path_complet):
                files.append(path_complet)
        print(f"Files listed on folder'{folder}: {files}")
    except Exception as e:
        print(f"Error to list files on folder '{folder}': {e}")
        raise
    return files


def upload_files_to_s3(files: List[str]) -> None:
    for file in files:
        file_name: str = os.path.basename(file)
        try:
            s3_client.upload_file(file, BUCKET_NAME, file_name)
            print(f'{file_name} sent to S3.')
        except Exception as e:
            print(f"Error on sending '{file_name}' to S3.")
            raise
        

def delete_local_file(files: List[str]) -> None:
    for file in files:
        try:
            os.remove(file)
            print(f'{file} was deleted from local folder.')
        except Exception as e:
            print(f"Error on deleting file '{file}' :{e}")
            raise


def execute_backup(folder: str) -> None:
    try:
        files: List[str] = list_files(folder)
        if files:
            upload_files_to_s3(files)
            delete_local_file(files)

        else:
            print("No file found for backup.")
    except Exception as e:
        print(f"Error on processing backup: {e}")
        raise


if __name__ == '__main__':
    LOCAL_FOLDER: str = 'download'
    try:
        execute_backup(LOCAL_FOLDER)
    except Exception as e:
        print(f"Error executing backup: {e}")