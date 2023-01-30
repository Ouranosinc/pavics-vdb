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
from .. import ncml
from ..datamodels.base import Common

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

        attributes = [{"column_name": key} for key in self.cv.attributes()]
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
        return self.cv.global_attributes() + self.cv.variable_attributes()

    def to_row(self, attrs) -> List:
        """Return attribute values."""
        return [attrs[k] for k in self.cv.global_attributes()] + \
               [[vd[k] for vd in attrs["variables"].values()] for k in self.cv.variable_attributes()]

    def get_attrs(self, xml: bytes) -> dict:
        """Extract metadata attributes defined by CV from NcML file.

        Parameters
        ----------
        xml : bytes
          NcML content.
        """
        # Create Element Node
        elem = ncml.to_element(xml)

        # Parse and validate datamodel
        dm = self.cv.from_orm(elem)

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

        if len(out) == 1:
            raise ValueError("No valid data in table. Check logs for parsing errors.")

        return out

    def save(self, catalog, path='.', name=None):
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

        """
        import csv
        import json
        import gzip
        from pathlib import Path

        path = Path(path)
        name = name or self.cid

        # Write ESM-Collection json file
        with open(path / f"{name}.json", "w") as f:
            json.dump(self.to_intake_spec(name), f)

        # Write catalog data in csv.gz format
        with gzip.open(filename=path / f"{name}.csv.gz", mode="wt") as f:
            w = csv.writer(f)
            w.writerow(self.header())
            for row in catalog:
                w.writerow(row)
