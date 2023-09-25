import os

from pathlib import Path
from dotenv import load_dotenv
import pickle
import boto3
import re

from .config import get_config


config = get_config()

dotenv_path = os.path.join(Path(__file__).parents[1], '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')


def get_session_s3(config: dict):
    session = boto3.Session(
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=config['s3_region'],
    )
    s3_client = session.client("s3", endpoint_url=config['s3_endpoint'])
    return s3_client


def save_artifacts_s3(file, path_local: str, path_s3: str) -> None:
    s3 = get_session_s3(config)

    with open(path_local, 'wb') as f:
        pickle.dump(file, f)

    s3.upload_file(path_local, config['artifacts_s3'], path_s3)


def load_artifacts_s3(path_s3: str):
    s3 = get_session_s3(config)

    get_object_response = s3.get_object(Bucket=config['s3_bucket'], Key=path_s3)
    file = get_object_response['Body'].read()
    return file


def parse_filenames(filenames: list[dict]) -> dict:
    files = {}
    for key in filenames:
        file = os.path.basename(key['Key'])
        res = re.search(r'20\d{2}-[01]?\d', file)
        if res is not None:
            files[res[0]] = file
    return files


def get_new_file_paths(config: dict) -> list[str]:
    s3 = get_session_s3(config)

    pattern = r"\b[a-zA-Z0-9-_]+[\/]$"
    prefix_raw = re.search(pattern, config['paths']['raw_data_s3'])[0]
    prefix_clean = re.search(pattern, config['paths']['cleaned_data_s3'])[0]

    raw_objs = s3.list_objects(Bucket=config['s3_bucket'], Prefix=prefix_raw)
    cleaned_objs = s3.list_objects(Bucket=config['s3_bucket'], Prefix=prefix_clean)

    if 'Contents' in raw_objs:
        raw_data = parse_filenames(raw_objs['Contents'])
    else:
        raw_data = {}

    if 'Contents' in cleaned_objs:
        cleaned_data = parse_filenames(cleaned_objs['Contents'])
    else:
        cleaned_data = {}

    new_files = set()
    for file in raw_data:
        if file not in cleaned_data:
            new_files.add(os.path.join(config['paths']['raw_data_s3'], raw_data[file]))

    return new_files


if __name__ == '__main__':
    paths = get_new_file_paths(config)
