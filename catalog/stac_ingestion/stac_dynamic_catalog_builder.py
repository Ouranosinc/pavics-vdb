from urllib.parse import urljoin
from utils import bcolors

import requests
import json
import os


class StacDynamicCatalogBuilder(object):
    def build(self, catalog_output_path, stac_host):
        """
        Build dynamic STAC catalog.
        Post items to STAC_HOST.

        :param catalog_output_path:
        :param stac_host:
        :return:
        """
        # each collection
        for root, dirs, _ in os.walk(catalog_output_path):
            col_path = root
            col_file = col_path + "/collection.json"

            if os.path.exists(col_file):
                print(f"[INFO] Processing collection [{col_file}]")

                collection_id = self.post_collection(col_file, stac_host)

                # each item
                for root, dirs, _ in os.walk(col_path):
                    for d in dirs:
                        item_path = col_path + "/" + d
                        item_file = os.listdir(item_path)[0]    # assume only one STAC item per pystac item folder
                        print(f"[INFO] Processing item {d}")
                        self.post_collection_item(item_path + "/" + item_file, stac_host, collection_id)


    def post_collection(self, file_path, stac_host):
        """
        Post a STAC collection.

        Returns the collection id.
        """
        with open(file_path, 'r') as file:
            json_data = json.load(file)
            collection_id = json_data['id']
            r = requests.post(urljoin(stac_host, "/collections"), json=json_data)

            if r.status_code == 200:
                print(f"{bcolors.OKGREEN}[INFO] Created collection [{collection_id}] ({r.status_code}){bcolors.ENDC}")
            elif r.status_code == 409:
                print(f"{bcolors.WARNING}[INFO] Collection already exists [{collection_id}] ({r.status_code}), updating..{bcolors.ENDC}")
                r = requests.put(urljoin(stac_host, "/collections"), json=json_data)
                r.raise_for_status()
            else:
                r.raise_for_status()

        return collection_id


    def post_collection_item(self, file_path, stac_host, collection_id):
        """
        Post an item to a collection.
        """
        with open(file_path, 'r') as file:
            json_data = json.load(file)
            item_id = json_data['id']
            r = requests.post(urljoin(stac_host, f"/collections/{collection_id}/items"), json=json_data)

            if r.status_code == 200:
                print(f"{bcolors.OKGREEN}[INFO] Created item [{item_id}] ({r.status_code}){bcolors.ENDC}")
            elif r.status_code == 409:
                print(f"{bcolors.WARNING}[INFO] Item already exists [{item_id}] ({r.status_code}), updating..{bcolors.ENDC}")
                r = requests.put(urljoin(stac_host, f"/collections/{collection_id}/items"), json=json_data)
                r.raise_for_status()
            else:
                r.raise_for_status()
