from urllib.parse import urljoin

import requests
import json

STAC_HOST = "http://132.217.140.135:8081/"


def stac_ingest():
    collection_id = post_collection("./test_collection.json", STAC_HOST)
    post_collection_item("./test_item.json", STAC_HOST, collection_id)    


def post_collection(file_path, stac_host):
    """
    Post a STAC collection.

    Returns the collection id.
    """
    collection_id = "none"

    with open(file_path, 'r') as file:
        json_data = json.load(file)
        collection_id = json_data['id']
        r = requests.post(urljoin(stac_host, "/collections"), json=json_data)

        if r.status_code == 200:
            print(f"{bcolors.OKGREEN}[INFO] Created collection [{collection_id}] ({r.status_code}){bcolors.ENDC}")
        elif r.status_code == 409:
            # TODO : put instead, since collection exists
            print(f"{bcolors.WARNING}[INFO] Collection already exists [{collection_id}] ({r.status_code}){bcolors.ENDC}")
        else:
            r.raise_for_status()

    return collection_id


def post_collection_item(file_path, stac_host, collection_id):
    """
    Post an item to a collection.
    """
    # TODO : handle multiple Features from single file
    with open(file_path, 'r') as file:
        json_data = json.load(file)
        item_id = json_data['id']
        r = requests.post(urljoin(stac_host, f"/collections/{collection_id}/items"), json=json_data)

        if r.status_code == 200:
            print(f"{bcolors.OKGREEN}[INFO] Created item [{item_id}] ({r.status_code}){bcolors.ENDC}")
        elif r.status_code == 409:
            # TODO : put instead, since collection exists
            print(f"{bcolors.WARNING}[INFO] Item already exists [{item_id}] ({r.status_code}){bcolors.ENDC}")
        else:
            r.raise_for_status()


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


if __name__ == "__main__":
    stac_ingest()