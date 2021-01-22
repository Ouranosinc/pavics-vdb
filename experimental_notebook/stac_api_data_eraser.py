import requests
from urllib.parse import urljoin

from experimental_notebook.utils import bcolors

stac_host = "http://132.217.140.135:8081/"


def destroy_stac_data():
    """
    Will delete all STAC API data, for tests.
    """
    r = requests.get(urljoin(stac_host, "/collections"))
    collections = r.json()

    for c in collections:
        r = requests.get(urljoin(stac_host, f"/collections/{c['id']}/items"))
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