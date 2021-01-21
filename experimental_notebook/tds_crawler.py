from siphon.catalog import TDSCatalog
from datetime import datetime

import json
import xarray as xr
import netCDF4
import pystac
import os

# TODO : run stac-validator


def main():
    crawl_tds()

    # create STAC catalog
    collection_items = [get_collection_item()]
    collection = get_collection(collection_items)
    catalog = get_catalog(collection)

    # normalize and save
    CATALOG_SAVE_PATH=os.getcwd() + "/output"
    print("[INFO] Save path : " + CATALOG_SAVE_PATH)
    catalog.describe()
    catalog.normalize_hrefs(CATALOG_SAVE_PATH)
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)


def parse_datasets(catalog):
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

        item = add_tds_ds_metadata(item)
        datasets.append(item)
        
        # TODO remove
        break
        
    for catalog_name, catalog_obj in catalog.catalog_refs.items():
        d = parse_datasets(catalog_obj.follow())
        datasets.extend(d)
        
        # TODO : remove, only to have a small dataset
        break

    return datasets

    
def crawl_tds():
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
        tds_ds = parse_datasets(top_cat)
        with open(CACHE_FILEPATH, "w") as data_file:
            json.dump(tds_ds, data_file, indent=4)
    else:
        print("[INFO] Use testdata")
        tds_ds = [{"dataset_name": "BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "http_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/fileServer/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "odap_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/dodsC/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "ncml_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/ncml/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "uddc_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/uddc/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "iso_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/iso/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "wcs_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/wcs/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc", "wms_url": "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/wms/birdhouse/cccs_portal/indices/Final/BCCAQv2/tx_mean/allrcps_ensemble_stats/YS/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_tx_mean_YS.nc"}]
    
    print("finished creating all STAC items")
    
    
def add_tds_ds_metadata(ds):
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
    

def get_collection_item():
    collection_item = pystac.Item(id='local-image-col-1',
                               geometry={},
                               bbox={},
                               datetime=datetime.utcnow(),
                               properties={},
                               stac_extensions=[pystac.Extensions.COLLECTION_ASSETS])

    collection_item.common_metadata.gsd = 0.3
    collection_item.common_metadata.platform = 'Maxar'
    collection_item.common_metadata.instruments = ['WorldView3']
    
    # asset = pystac.Asset(href=img_path, 
    #                       media_type=pystac.MediaType.GEOTIFF)
    # collection_item.add_asset('image', asset)
    
    return collection_item


def get_collection(collection_items):
    # extents
    sp_extent = pystac.SpatialExtent([None,None,None,None])
    capture_date = datetime.strptime('2015-10-22', '%Y-%m-%d')
    tmp_extent = pystac.TemporalExtent([(capture_date, None)])
    extent = pystac.Extent(sp_extent, tmp_extent)
    
    collection = pystac.Collection(id='tx-mean',
                                   description='tx mean',
                                   extent=extent,
                                   license='CC-BY-SA-4.0')

    collection.add_items(collection_items)
    
    return collection


def get_catalog(collection):
    catalog = pystac.Catalog(id='bccaqv2', description='BCCAQv2 STAC')
    
    catalog.clear_items()
    catalog.clear_children()
    catalog.add_child(collection)
    
    return catalog


if __name__ == "__main__":
    main()