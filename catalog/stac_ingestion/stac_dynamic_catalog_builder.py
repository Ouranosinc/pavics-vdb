from urllib.parse import urljoin
from utils import bcolors

import requests
import json
import os


class StacDynamicCatalogBuilder(object):
    def build(self, catalog_output_path, stac_host):
        # each collection
        for i in os.listdir(catalog_output_path):
            col_path = catalog_output_path + "/" + i

            if os.path.isdir(col_path):
                print(f"[INFO] Processing collection {i}")

                collection_id = self.post_collection(col_path + "/collection.json", stac_host)

                # each item
                for root, dirs, _ in os.walk(col_path):
                    for d in dirs:
                        item_path = col_path + "/" + d
                        item_file = os.listdir(item_path)[0]
                        print(f"[INFO] Processing item {d}")
                        self.post_collection_item(item_path + "/" + item_file, stac_host, collection_id)



        # collection_id = self.post_collection("./test_collection.json", stac_host)
        # self.post_collection_item("./test_item.json", stac_host, collection_id)


    def post_collection(self, file_path, stac_host):
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


    def post_collection_item(self, file_path, stac_host, collection_id):
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
