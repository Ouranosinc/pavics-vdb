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
from collections import defaultdict
from siphon.catalog import TDSCatalog
from xml.etree.ElementTree import ParseError
from dataclasses import dataclass, fields, asdict, astuple
import tds

ESMCAT_VERSION = "0.1.0"


class Intake:
    """Class that, combined with a CV class, facilitates the creation of Intake catalogs."""
    asset_attribute = "path"
    asset_format = "netcdf"

    def __init__(self, cv):
        """
        Parameters
        ----------
        cv : CV subclass
          CV subclass defining the controlled vocabulary for the catalog to be created.
        """
        self.cv = cv

    def to_intake_spec(self):
        """Return Intake specification file content."""

        attributes = [{"column_name": key} for key in self.cv.global_attributes() + self.cv.variable_attributes()]
        spec = {"esmcat_version": ESMCAT_VERSION,
                "id": self.cv.__name__.lower(),
                "description": self.cv.__doc__.splitlines()[0],
                "catalog_file": self.catalog_fn,
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

    @property
    def catalog_fn(self):
        """Return catalog file name."""
        return f"{self.cid}.csv.gz"

    def header(self):
        """Return attribute table header names."""
        return self.cv.global_attributes() + self.cv.variable_attributes() + [self.asset_attribute]

    def validate(self, **kwargs):
        """Return an instance of the CV class, which is responsible for validating the metadata."""
        return self.cv(**kwargs)

    def aslist(self, entry, asset):
        """Return tuple of values."""
        d = asdict(entry)
        d[self.asset_attribute] = asset
        return [d[k] for k in self.header()]

    def parse(self, url):
        """Return a table including a header and rows of attribute values based on CV.
        """
        try:
            cat = TDSCatalog(url)
        except ParseError as err:
            raise ConnectionError(f"Could not open {url}\n") from err

        table = [self.header()]
        for name, ds in tds.walk(cat, depth=None):
            attrs = tds.attrs_from_ds(ds)

            # Global attributes
            g_val = {k: attrs.get(k, "NA") for k in self.cv.global_attributes()}

            # Variable attributes (lists)
            v_val = defaultdict(list)
            for key in self.cv.variable_attributes():
                k = key.split("variable_")[1]
                for vk, vd in attrs["__variable__"].items():
                    if k in vd:
                        v_val[key].append(vd[k])
                    elif k == "name":
                        v_val[key].append(vk)
                    else:
                        v_val[key].append("NA")

            # Spec validation
            entry = self.validate(**g_val, **v_val)

            # Asset path
            asset = attrs["__services__"]["OPENDAP"]

            row = self.aslist(entry, asset=asset)
            table.append(row)

        return table

    def to_catalog(self, table, path='.'):
        """Write catalog table to disk."""
        import csv
        import json
        import gzip
        from pathlib import Path

        path = Path(path)

        # Write spec
        with open(path / f"{self.cid}.json", "w") as f:
            json.dump(self.to_intake_spec(), f)

        # Write table in csv.gz format
        with gzip.open(filename=path / self.catalog_fn, mode="wt") as f:
            w = csv.writer(f)
            for row in table:
                w.writerow(row)
