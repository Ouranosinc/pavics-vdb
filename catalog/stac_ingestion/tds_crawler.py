from siphon.catalog import TDSCatalog
from metadata_validator import MetadataValidator, REGISTERED_SCHEMAS, OBJECT_TYPE, SCHEMAS
from colorlog import ColoredFormatter

# TODO : hackish import from parent folder
import sys
sys.path.append('..')
import tds
import logging


# setup logger
LOGGER = logging.getLogger(__name__)
LOGFORMAT = "  %(log_color)s%(levelname)-8s%(reset)s | %(log_color)s%(message)s%(reset)s"
formatter = ColoredFormatter(LOGFORMAT)
stream = logging.StreamHandler()
stream.setFormatter(formatter)
LOGGER.addHandler(stream)
LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False


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

            LOGGER.info("[INFO] Found TDS dataset [%s]", dataset_name)
            item = self.add_tds_ds_metadata(item)
            item_metadata_schema_uri = REGISTERED_SCHEMAS[SCHEMAS.CMIP5][OBJECT_TYPE.SCHEMA]
            item_metadata_root = REGISTERED_SCHEMAS[SCHEMAS.CMIP5][OBJECT_TYPE.ROOT]
            metadata_validator = MetadataValidator()
            is_valid = metadata_validator.is_valid(item, item_metadata_schema_uri, item_metadata_root)

            if is_valid:
                datasets.append(item)
                LOGGER.info("[INFO] Valid dataset [%s]", dataset_name)
            else:
                LOGGER.warning("[WARNING] Invalid dataset [%s]", dataset_name)

        for catalog_name, catalog_obj in catalog.catalog_refs.items():
            d = self.parse_datasets(catalog_obj.follow())
            datasets.extend(d)

        return datasets


    def add_tds_ds_metadata(self, ds):
        """
        Add extra metadata to item.
        """
        # TODO : hardcoded, replace with regexes
        # TODO : Extract metadata from ncml_url and iso_url
        url_attrs = ds["http_url"].split("/")
        ds_attrs = ds["dataset_name"].split("_")
        ncml_attrs = tds.attrs_from_ncml(ds["ncml_url"])

        # # cmip5 sample
        # extra_meta = {
        #     "provider": "pavics-thredds",
        #     "activity_id": ncml_attrs.get("project_id", ""),
        #     "institution_id": ncml_attrs.get("institute_id", ""),
        #     "source_id": "na",
        #     "experiment_id": ncml_attrs.get("driving_experiment", ""),
        #     "member_id": "na",
        #     "table_id": "na",
        #     "variable_id": "",
        #     "grid_label": "na",
        #     "conventions": ncml_attrs.get("Conventions", ""),
        #     "frequency": ncml_attrs.get("frequency", url_attrs[15]),
        #     "modeling_realm": ncml_attrs.get("modeling_realm", ""),
        #     "model_id": ncml_attrs.get("model_id", "")
        # }

        # cmip6 sample
        extra_meta = {
            "provider": "pavics-thredds",
            "activity_id": "CMIP",
            "experiment_id": "1pctCO2",
            "frequency": "mon",
            "grid_label": "gm",
            "institution_id": "CCCma",
            "license": "none",
            "nominal_resolution": "1 km",
            "realm": "atmos",
            "source_id": "4AOP-v1-5",
            "source_type": "AER",
            "sub_experiment_id": "s1910",
            "table_id": "3hr"
        }

        return dict(ds, **extra_meta)
