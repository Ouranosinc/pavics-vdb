from .collection_base import CollectionBase

# TODO : hackish import from parent folder
import sys
sys.path.append('../..')
import tds


class ThreddsCmip5(CollectionBase):
    """
    Implementation of Thredds CMIP5 metadata processing logic.
    """

    def add_metadata(self, ds):
        """
        Add extra metadata to item.
        """
        # TODO : hardcoded, replace with regexes
        # TODO : Extract metadata from ncml_url and iso_url
        url_attrs = ds["http_url"].split("/")
        ds_attrs = ds["dataset_name"].split("_")
        ncml_attrs = tds.attrs_from_ncml(ds["ncml_url"])

        # cmip5 sample
        extra_meta = {
            "provider": "pavics-thredds",
            "activity_id": ncml_attrs.get("project_id", ""),
            "institution_id": ncml_attrs.get("institute_id", ""),
            "source_id": "na",
            "experiment_id": ncml_attrs.get("driving_experiment", ""),
            "member_id": "na",
            "table_id": "na",
            "variable_id": "",
            "grid_label": "na",
            "conventions": ncml_attrs.get("Conventions", ""),
            "frequency": ncml_attrs.get("frequency", url_attrs[15]),
            "modeling_realm": ncml_attrs.get("modeling_realm", ""),
            "model_id": ncml_attrs.get("model_id", "")
        }

        return dict(ds, **extra_meta)

    def get_stac_collection(self, item):
        pass

    def get_stac_collection_item(self, collection_items, collection_name):
        pass

