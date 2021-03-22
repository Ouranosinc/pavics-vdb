from urllib.parse import urljoin
from utils import bcolors
from dotenv import load_dotenv

import os
import requests

load_dotenv()


def destroy_stac_data():
    """
    Will delete all STAC API data, for tests.
    """
    stac_host = os.getenv("STAC_HOST")
    r = requests.get(urljoin(stac_host, "/collections"))
    collections = r.json()

    for c in collections:
        r = requests.get(urljoin(stac_host, f"/collections/{c['id']}/items?limit=99999999"))
        items = r.json()

        for i in items['features']:
            r = requests.delete(urljoin(stac_host, f"/collections/{c['id']}/items/{i['id']}"))
            if r.status_code == 200:
                print(f"{bcolors.OKGREEN}[INFO] Deleted [/collections/{c['id']}/items/{i['id']}] ({r.status_code}){bcolors.ENDC}")
            else:
                r.raise_for_status()

        r = requests.delete(urljoin(stac_host, f"/collections/{c['id']}"))
        if r.status_code == 200:
            print(f"{bcolors.OKGREEN}[INFO] Deleted [/collections/{c['id']}] ({r.status_code}){bcolors.ENDC}")
        else:
            r.raise_for_status()

    print(f"{bcolors.OKGREEN}[INFO] No more data in STAC API Catalog{bcolors.ENDC}")

if __name__ == "__main__":
    destroy_stac_data()