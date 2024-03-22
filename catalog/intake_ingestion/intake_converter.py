"""
Create intake catalog from NcML attributes.

requirements = siphon intake intake-esm lxml

Usage example
-------------

import intake
cat = intake.open_esm_datastore("reanalysis.json")
cat.df

# Notes
https://intake-esm.readthedocs.io/en/latest/user-guide/multi-variable-assets.html

"""
from typing import List, Iterator
from pydantic import ValidationError
from loguru import logger
import xncml


ESMCAT_VERSION = "0.1.0"


class Intake:
    """Class that, combined with a CV class, facilitates the creation of Intake catalogs."""
    asset_attribute = "path"
    asset_format = "netcdf"

    def __init__(self, cv):
        """
        Parameters
        ----------
        cv : `datamodels.base.Common` subclass
          Data model for the catalog entries.
        """
        self.cv = cv

    def to_intake_spec(self, name=None, description=""):
        """Return Intake specification file content."""

        # Define column names

        attributes = [{"column_name": key} for key in self.header()]
        spec = {"esmcat_version": ESMCAT_VERSION,
                "id": self.cv.__name__.lower(),
                "description": description,
                "catalog_file": f"{name}.csv.gz",
                "attributes": attributes,
                "assets": {
                    "column_name": self.asset_attribute,
                    "format": self.asset_format
                }
                }
        return spec

    @property
    def cid(self):
        """Return class ID."""
        return self.cv.__name__.lower()

    def header(self) -> List:
        """Return attribute names."""
        columns = get_field_names(self.cv, "attributes")
        columns += [f"var_{k}" for k in get_field_names(self.cv, "variables")]
        return columns

    def to_row(self, attrs) -> List:
        """Return attribute values in a flat row."""

        # Global attributes
        g_fields = get_field_names(self.cv, "attributes")
        out = [attrs["attributes"][k] for k in g_fields]

        # Variable attributes
        variables = attrs["variables"].values()
        v_fields = get_field_names(self.cv, "variables")
        for k in v_fields:
            out.append([v[k] for v in variables])

        return out

    def get_attrs(self, xml: bytes) -> dict:
        """Extract metadata attributes defined by CV from NcML file.

        Parameters
        ----------
        xml : bytes
          NcML content.
        """
        from tempfile import NamedTemporaryFile
        # Create Element Node
        f = NamedTemporaryFile()
        f.write(xml)
        attrs = xncml.Dataset(f.name).to_cf_dict()

        # Parse and validate datamodel
        dm = self.cv(**attrs)

        return dm.dict()

    def catalog(self, iterator: Iterator) -> List:
        """Create catalog entries by walking through iterator.

        Parameters
        ----------
        iterator : iterable
          Sequence of (name: str, xml: bytes)
        """
        out = []

        for name, item in iterator:

            try:
                attrs = self.get_attrs(item)
                out.append(self.to_row(attrs))

            except ValidationError as exc:
                logger.warning(f"Metadata error in {name}:\n {exc}")

        if len(out) == 0:
            logger.warning("Empty table.")

        return out

    def save(self, catalog, path='.', name=None) -> str:
        """Write catalog table to disk.

        An Intake-esm catalog has two pieces, an ESM-Collection json file that provides metadata about the catalog,
        and a catalog csv file that lists the catalog content.

        Parameters
        ----------
        catalog : list
          Metadata table.
        path : str
          Directory where catalog files should be written.
        name : str
          Catalog name (no extension). Defaults to the data model name.

        Returns
        -------
        str
          Filename of catalog json description.
        """
        import csv
        import json
        import gzip
        from pathlib import Path

        path = Path(path)
        name = name or self.cid

        # Write ESM-Collection json file
        fn = path / f"{name}.json"
        with open(fn, "w") as f:
            json.dump(self.to_intake_spec(name), f)

        # Write catalog data in csv.gz format
        with gzip.open(filename=path / f"{name}.csv.gz", mode="wt") as f:
            w = csv.writer(f)
            w.writerow(self.header())
            for row in catalog:
                w.writerow(row)

        return fn

def get_field_names(cls, attr: str) -> list:
    """List of attribute names for given attribute (assuming it's another BaseModel)."""
    model = cls.__fields__[attr]
    if model.sub_fields:
        _check_fields_identical(model.sub_fields)
        model = model.sub_fields[0]
    return list(model.type_.__fields__.keys())


def _check_fields_identical(fields):
    attrs = []
    for field in fields:
        attrs.append(tuple(field.type_.__fields__.keys()))
    if len(set(attrs)) > 1:
        raise AttributeError("Fields are not identical across submodels.")
