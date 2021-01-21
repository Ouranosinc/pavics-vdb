from datetime import datetime

import os
import pystac
import pystac.extensions.eo

# TODO : run stac-validator


class StacCatalogBuilder(object):
    def build(self, metadata):
        collection_items = []

        for i, item in enumerate(metadata):
            stac_collection_item = self.get_collection_item(item)

            collection_items.append(stac_collection_item)

        collection = self.get_collection(collection_items)
        catalog = self.get_catalog(collection)

        self.persist(catalog)


    def persist(self, catalog):
        # normalize and save
        CATALOG_SAVE_PATH = os.getcwd() + "/output"
        print("[INFO] Save path : " + CATALOG_SAVE_PATH)
        catalog.describe()
        catalog.normalize_hrefs(CATALOG_SAVE_PATH)
        catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)


    def get_collection_item(self, item):
        collection_item = pystac.Item(id=item["dataset_name"],
                                      geometry={},
                                      bbox={},
                                      datetime=datetime.utcnow(),
                                      properties={},
                                      stac_extensions=[pystac.Extensions.DATACUBE])

        collection_item.properties["meta:provider"] = "thredds"
        collection_item.properties["cmip5:activity_id"] = ""
        collection_item.properties["cmip5:institution_id"] = "CCCS"
        collection_item.properties["cmip5:source_id"] = ""
        collection_item.properties["cmip5:experiment_id"] = "historical,rcp26,rcp45,rcp85"
        collection_item.properties["cmip5:member_id"] = ""
        collection_item.properties["cmip5:table_id"] = "Table day (10 Jun 2010)"
        collection_item.properties["cmip5:variable_id"] = "tx_mean"
        collection_item.properties["cmip5:grid_label"] = ""
        collection_item.properties["cmip5:conventions"] = "CF-1.4"
        collection_item.properties["cmip5:frequency"] = "day"
        collection_item.properties["cmip5:modeling_realm"] = "atmos"

        link = pystac.Link("file", item["http_url"], "application/netcdf")
        collection_item.add_link(link)

        asset = pystac.Asset(href=item["iso_url"], media_type="application/xml", title="Metadata ISO")
        collection_item.add_asset('metadata_iso', asset)

        asset = pystac.Asset(href=item["ncml_url"], media_type="application/xml", title="Metadata NcML")
        collection_item.add_asset('metadata_ncml', asset)

        return collection_item

    def get_collection(self, collection_items):
        # extents
        sp_extent = pystac.SpatialExtent([None, None, None, None])
        capture_date = datetime.strptime('2015-10-22', '%Y-%m-%d')
        tmp_extent = pystac.TemporalExtent([(capture_date, None)])
        extent = pystac.Extent(sp_extent, tmp_extent)

        collection = pystac.Collection(id='tx-mean',
                                       description='tx mean',
                                       extent=extent,
                                       license='CC-BY-SA-4.0')

        collection.add_items(collection_items)

        return collection

    def get_catalog(self, collection):
        catalog = pystac.Catalog(id='bccaqv2', description='BCCAQv2 STAC')

        catalog.clear_items()
        catalog.clear_children()
        catalog.add_child(collection)

        return catalog