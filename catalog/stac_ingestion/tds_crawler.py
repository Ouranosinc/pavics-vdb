from siphon.catalog import TDSCatalog
from utils import bcolors

# TODO : hackish import from parent folder
import sys
sys.path.append('..')
import tds

class TDSCrawler(object):
    def run(self, tds_catalog_url):
        """
        Crawl TDS.

        :param tds_catalog_url:
        :return:
        """
        top_cat = TDSCatalog(tds_catalog_url)
        tds_ds = self.parse_datasets(top_cat)

        print("[INFO] Finished crawling TDS")

        return tds_ds

    def parse_datasets(self, catalog):
        """
        Collect all available datasets.
        """
        datasets = []

        for dataset_name, dataset_obj in catalog.datasets.items():
            http_url = dataset_obj.access_urls.get("httpserver", "")
            odap_url = dataset_obj.access_urls.get("opendap", "")
            ncml_url = dataset_obj.access_urls.get("ncml", "")
            uddc_url = dataset_obj.access_urls.get("uddc", "")
            iso_url = dataset_obj.access_urls.get("iso", "")
            wcs_url = dataset_obj.access_urls.get("wcs", "")
            wms_url = dataset_obj.access_urls.get("wms", "")

            # TODO : harcoded!
            collection_name = catalog.catalog_url[97:-12].replace("/", "_") # replace to avoid having slash in collection name url

            item = {
                "dataset_name" : dataset_name.split(".")[0],
                "collection_name" : collection_name,
                "http_url" : http_url,
                "odap_url" : odap_url,
                "ncml_url" : ncml_url,
                "uddc_url" : uddc_url,
                "iso_url" : iso_url,
                "wcs_url" : wcs_url,
                "wms_url" : wms_url
            }

            print(f"{bcolors.OKGREEN}[INFO] Found TDS dataset [{dataset_name}]{bcolors.ENDC}")
            item = self.add_tds_ds_metadata(item)

            # TODO : validate cmip5 schema with CV
            # TODO : datasets.append(item) only if valid schema, otherwise log exception

            datasets.append(item)

        for catalog_name, catalog_obj in catalog.catalog_refs.items():
            d = self.parse_datasets(catalog_obj.follow())
            datasets.extend(d)

        return datasets


    def add_tds_ds_metadata(self, ds):
        """
        Add extra metadata to item.
        """
        # TODO : hardcoded, especially for url_attrs. Replace with regexes
        # TODO : Extract metadata from ncml_url and iso_url
        url_attrs = ds["http_url"].split("/")
        ds_attrs = ds["dataset_name"].split("_")

        ncml_attrs = tds.attrs_from_ncml(ds["ncml_url"])

        # cv = {
        #     "activity_id": "CMIP5",
        #     "institution_id": "CCCma",
        #     "source_id": "na",
        #     "experiment_id": "",  # url_attrs[14],
        #     "member_id": "na",
        #     "table_id": "na",
        #     "variable_id": "",  # url_attrs[13],
        #     "grid_label": "na",
        #     "conventions": "na",
        #     "frequency": "",  # url_attrs[15],
        #     "modeling_realm": ""  # ds_attrs[0]
        # }

        extra_meta = {
            "provider": "pavics-thredds",
            "activity_id": "CMIP5",
            "institution_id": "CCCS",
            "source_id": "na",
            "experiment_id": "", # url_attrs[14],
            "member_id": "na",
            "table_id": "na",
            "variable_id": "",  # url_attrs[13],
            "grid_label": "na",
            "conventions": "na",
            "frequency": "", # url_attrs[15],
            "modeling_realm": ""  # ds_attrs[0]
        }

        return dict(ds, **extra_meta)
