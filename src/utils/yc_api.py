import os
from typing import Union

from dotenv import load_dotenv
from pathlib import Path
import requests


dotenv_path = os.path.join(Path(__file__).parents[1], '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

OAUTH_TOKEN = os.environ.get('OAUTH_TOKEN')
FOLDER_ID = os.environ.get('FOLDER_ID')


def get_iam_token():
    req_data = {"yandexPassportOauthToken": f"{OAUTH_TOKEN}"}
    url = "https://iam.api.cloud.yandex.net/iam/v1/tokens"

    response = requests.post(url, json=req_data)
    response = response.json()

    return response['iamToken']


def get_masternode_ip() -> Union[None, str]:
    iam_token = get_iam_token()

    headers = {"Authorization": f"Bearer {iam_token}"}
    params = {'folderId': FOLDER_ID}

    url = 'https://compute.api.cloud.yandex.net/compute/v1/instances'
    response = requests.get(url, headers=headers, params=params)
    response = response.json()

    for instance in response['instances']:
        labels = instance.get('labels')

        if labels:
            if labels['yc.service.originproduct'] == 'dataproc':
                if labels['subcluster_role'] == 'masternode':
                    ip = instance['networkInterfaces'][0]['primaryV4Address']['oneToOneNat']['address']
                    return ip


def get_host_ip() -> Union[None, str]:
    iam_token = get_iam_token()

    headers = {"Authorization": f"Bearer {iam_token}"}
    params = {'folderId': FOLDER_ID}

    url = 'https://compute.api.cloud.yandex.net/compute/v1/instances'
    response = requests.get(url, headers=headers, params=params)
    response = response.json()

    for instance in response['instances']:
        labels = instance.get('labels')

        if labels is None:
            ip = instance['networkInterfaces'][0]['primaryV4Address']['oneToOneNat']['address']
            return ip


if __name__ == '__main__':
    iam_token = get_iam_token()
    print(get_masternode_ip())
    print(get_host_ip())


