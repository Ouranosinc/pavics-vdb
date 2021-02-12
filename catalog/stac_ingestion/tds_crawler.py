from siphon.catalog import TDSCatalog
from utils import bcolors


class TDSCrawler(object):
    def run(self, tds_catalog_url):
        """
        Crawl TDS.
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

            item = {
                "dataset_name" : dataset_name.split(".")[0],
                "http_url" : http_url,
                "odap_url" : odap_url,
                "ncml_url" : ncml_url,
                "uddc_url" : uddc_url,
                "iso_url" : iso_url,
                "wcs_url" : wcs_url,
                "wms_url" : wms_url
            }

            print(
                f"{bcolors.OKGREEN}[INFO] Found TDS dataset [{dataset_name}]{bcolors.ENDC}")
            item = self.add_tds_ds_metadata(item)
            datasets.append(item)

            # Uncomment to create smaller dataset
            # break

        for catalog_name, catalog_obj in catalog.catalog_refs.items():
            d = self.parse_datasets(catalog_obj.follow())
            datasets.extend(d)

            # Uncomment to create smaller dataset
            # break

        return datasets


    def add_tds_ds_metadata(self, ds):
        """
        Add extra metadata to item.
        """
        # TODO : hardcoded, especially for url_attrs. Replace with regexes
        # TODO : Extract metadata from ncml_url and iso_url
        url_attrs = ds["http_url"].split("/")
        ds_attrs = ds["dataset_name"].split("_")

        extra_meta = {
            "provider": "thredds",
            "activity_id": "na",
            "institution_id": "na",
            "source_id": "na",
            "experiment_id": url_attrs[14],
            "member_id": "na",
            "table_id": "na",
            "variable_id": url_attrs[13],
            "grid_label": "na",
            "conventions": "na",
            "frequency": url_attrs[15],
            "modeling_realm": ds_attrs[0]
        }

        return dict(ds, **extra_meta)
