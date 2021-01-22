from datetime import datetime

import pystac
import pystac.extensions.eo
from shapely.geometry import Polygon, mapping
import experimental_notebook.utils as utils

# TODO : run stac-validator


class StacStaticCatalogBuilder(object):
    def build(self, metadata, catalog_output_path, collection_name):
        collection_items = []

        for i, item in enumerate(metadata):
            stac_collection_item = self.get_collection_item(item)
            collection_items.append(stac_collection_item)

        collection = self.get_collection(collection_items, collection_name)
        catalog = self.get_catalog(collection)
        self.persist(catalog, catalog_output_path)


    def persist(self, catalog, catalog_output_path):
        # normalize and save
        print("[INFO] Save path : " + catalog_output_path)
        catalog.describe()
        catalog.normalize_hrefs(catalog_output_path)
        catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)


    def get_collection_item(self, item):
        # get bbox and footprint
        bounds = {
            "left" : 180,
            "bottom" : 180,
            "right" : 180,
            "top" : 1080
        }
        bbox = [bounds["left"], bounds["bottom"], bounds["right"], bounds["top"]]
        footprint = Polygon([
            [bounds["left"], bounds["bottom"]],
            [bounds["left"], bounds["top"]],
            [bounds["right"], bounds["top"]],
            [bounds["right"], bounds["bottom"]]
        ])

        collection_item = pystac.Item(id=item["dataset_name"].split(".")[0],
                                      geometry=mapping(footprint),
                                      bbox=bbox,
                                      datetime=datetime.utcnow(),
                                      properties={},
                                      stac_extensions=[pystac.Extensions.DATACUBE])

        # TODO : utils.MockDateTime() has been used since STAC API requires date in %Y-%m-%dT%H:%M:%SZ format while
        #  pystac.Item.datetime include the ms
        collection_item.datetime = utils.MockDateTime()
        collection_item.properties["start_datetime"] = "2020-10-15T13:51:21Z"
        collection_item.properties["end_datetime"] = "2020-10-15T13:51:21Z"
        collection_item.properties["created"] = "2020-11-04T06:15:26Z"
        collection_item.properties["updated"] = "2020-11-04T06:15:26Z"
        collection_item.properties["meta:provider"] = item["provider"]
        collection_item.properties["cmip5:activity_id"] = item["activity_id"]
        collection_item.properties["cmip5:institution_id"] = item["institution_id"]
        collection_item.properties["cmip5:source_id"] = item["source_id"]
        collection_item.properties["cmip5:experiment_id"] = item["experiment_id"]
        collection_item.properties["cmip5:member_id"] = item["member_id"]
        collection_item.properties["cmip5:table_id"] = item["table_id"]
        collection_item.properties["cmip5:variable_id"] = item["variable_id"]
        collection_item.properties["cmip5:grid_label"] = item["grid_label"]
        collection_item.properties["cmip5:conventions"] = item["conventions"]
        collection_item.properties["cmip5:frequency"] = item["frequency"]
        collection_item.properties["cmip5:modeling_realm"] = item["modeling_realm"]

        link = pystac.Link("file", item["http_url"], "application/netcdf")
        collection_item.add_link(link)

        asset = pystac.Asset(href=item["iso_url"], media_type="application/xml", title="Metadata ISO")
        collection_item.add_asset('metadata_iso', asset)

        asset = pystac.Asset(href=item["ncml_url"], media_type="application/xml", title="Metadata NcML")
        collection_item.add_asset('metadata_ncml', asset)

        return collection_item

    def get_collection(self, collection_items, collection_name):
        # extents
        sp_extent = pystac.SpatialExtent([180, 180, 180, 180])
        capture_date = datetime.strptime('2015-10-22', '%Y-%m-%d')
        tmp_extent = pystac.TemporalExtent([(capture_date, capture_date)])
        extent = pystac.Extent(sp_extent, tmp_extent)

        collection = pystac.Collection(id=collection_name,
                                       description='CMIP5 collection',
                                       extent=extent,
                                       license='na')

        collection.add_items(collection_items)

        return collection

    def get_catalog(self, collection):
        catalog = pystac.Catalog(id='bccaqv2', description='BCCAQv2 STAC')

        catalog.clear_items()
        catalog.clear_children()
        catalog.add_child(collection)

        return catalog