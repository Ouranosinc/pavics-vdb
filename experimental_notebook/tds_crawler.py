from siphon.catalog import TDSCatalog

import json
import os


class TDSCrawler(object):
    def run(self):
        self.crawl_tds()

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
                "dataset_name" : dataset_name,
                "http_url" : http_url,
                "odap_url" : odap_url,
                "ncml_url" : ncml_url,
                "uddc_url" : uddc_url,
                "iso_url" : iso_url,
                "wcs_url" : wcs_url,
                "wms_url" : wms_url
            }

            item = self.add_tds_ds_metadata(item)
            datasets.append(item)

            # TODO remove
            break

        for catalog_name, catalog_obj in catalog.catalog_refs.items():
            d = self.parse_datasets(catalog_obj.follow())
            datasets.extend(d)

            # TODO : remove, only to have a small dataset
            break

        return datasets


    def crawl_tds(self):
        """
        Crawl TDS.
        """

        tds_ds = ""
        CACHE_FILEPATH = "tds_cache.json"
        TEST_DATA = False

        if os.path.exists(CACHE_FILEPATH):
            print("[INFO] Use cache")
            with open(CACHE_FILEPATH, 'r') as file:
                tds_ds = json.load(file)
        elif not TEST_DATA:
            print("[INFO] Build and use cache")
            top_cat = TDSCatalog("https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/catalog.xml")
            tds_ds = self.parse_datasets(top_cat)
            with open(CACHE_FILEPATH, "w") as data_file:
                json.dump(tds_ds, data_file, indent=4)
        else:
            print("[INFO] Use testdata")
            tds_ds = [{"dataset_name": "BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "http_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/fileServer/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "odap_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/dodsC/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "ncml_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/ncml/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "uddc_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/uddc/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "iso_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/iso/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "wcs_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/wcs/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "wms_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/wms/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc"}]

        print("finished creating all STAC items")


    def add_tds_ds_metadata(self, ds):
        """
        Add extra metadata to item.
        """
        # TODO : replace with regexes
        # TODO : Extract metadata from ncml_url and iso_url
        extra_meta = {
            "model" : "BCCAQv2+ANUSPLIN300",
            "experiment" : "ensemble-percentiles",
            "frequency" : "YS",
            "modeling_realm" : "historical+allrcps",
            "mip_table" : "",
            "ensemble_member" : "",
            "version_number" : "",
            "variable_name" : "tx_mean",
            "temporal_subset" : "1950-2100"
        }

        return dict(ds, **extra_meta)
