from experimental_notebook.stac_static_catalog_builder import StacStaticCatalogBuilder
from experimental_notebook.tds_crawler import TDSCrawler
from experimental_notebook.stac_dynamic_catalog_builder import StacDynamicCatalogBuilder

import json
import os


def main():
    # PHASE I - TDS Crawler
    CACHE_FILEPATH = "tds_cache.json"
    TEST_DATA = False
    tds_catalog_url = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/catalog.xml"
    catalog_output_path = os.getcwd() + "/output"   # no trailing "/"
    stac_host = "http://132.217.140.135:8081/"
    collection_name = "cmip5_test_2"

    if os.path.exists(CACHE_FILEPATH):
        print("[INFO] Use cache")
        with open(CACHE_FILEPATH, 'r') as file:
            tds_ds = json.load(file)
    elif not TEST_DATA:
        print("[INFO] Build and use cache")
        tds_crawler = TDSCrawler()
        tds_ds = tds_crawler.run(tds_catalog_url)
        with open(CACHE_FILEPATH, "w") as data_file:
            json.dump(tds_ds, data_file, indent=4)
    else:
        print("[INFO] Use testdata")
        tds_ds = [
            {"dataset_name": "BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc",
             "http_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/fileServer/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc",
             "odap_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/dodsC/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc",
             "ncml_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/ncml/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc",
             "uddc_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/uddc/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc",
             "iso_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/iso/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc",
             "wcs_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/wcs/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc",
             "wms_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/wms/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc"}]

    # PHASE II - STAC Static Catalog Builder
    stacStaticCatalogBuilder = StacStaticCatalogBuilder()
    stacStaticCatalogBuilder.build(tds_ds, catalog_output_path, collection_name)

    # PHASE III - STAC API Dynamic Catalog Builder
    stacDynamicCatalogBuilder = StacDynamicCatalogBuilder()
    stacDynamicCatalogBuilder.build(catalog_output_path, stac_host)


if __name__ == "__main__":
    main()