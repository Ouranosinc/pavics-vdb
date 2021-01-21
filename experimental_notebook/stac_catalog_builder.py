import os
import pystac
from datetime import datetime

# TODO : run stac-validator


class StacCatalogBuilder(object):
    def build(self):
        # create STAC catalog
        collection_items = [self.get_collection_item()]
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


    def get_collection_item(self):
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