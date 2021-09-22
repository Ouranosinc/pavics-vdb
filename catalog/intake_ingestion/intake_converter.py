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
from dataclasses import asdict
from pydantic import ValidationError
import tds
import cv

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
        return self.cv.global_attributes() + \
               self.cv.dim_len_attributes() + \
               self.cv.variable_attributes()

    def aslist(self, entry):
        """Return tuple of values."""
        d = entry.dict()
        return [getattr(d[k], "value", d[k]) for k in self.header()]

    def parse(self, path):
        """Return a table including a header and rows of attribute values based on CV.

        Parameters
        ----------
        path : str, list
          URL pointing to a THREDDS catalog, or paths to directories storing netCDF files.
        """
        kind = "public" if isinstance(path, str) else "private"

        if kind == "public":
            walker = tds.walk_tds_ncml(path, depth=None)
        else:
            walker = tds.walk_disk_ncml(path)

        # Fill parameter table
        table = [self.header()]
        for ncml_attrs in walker:
            # Flatten dictionary of NcML attributes
            attrs = {}
            for gr, values in ncml_attrs.items():
                if gr == "variable":
                    # Special case to construct `variable` attributes
                    vars = []
                    for (k, v) in values.items():
                        try:
                            # Note that this filters out variables that do not match the spec
                            vars.append(cv.CFVariable(name=k, **v))
                        except ValidationError:
                            pass
                    for key in cv.CFVariable.__fields__:
                        attrs[f"variable_{key}"] = [getattr(v, key) for v in vars]
                else:
                    # All other attributes
                    # Warning, possibility of overwriting fields with the same key in different groups
                    attrs.update(values)

            # Asset path
            if kind == "public":
                asset = attrs["opendap"]  # Siphon stores this as a CaseInsentiveStr...
            else:
                asset = attrs["FS"]

            attrs[self.asset_attribute] = asset

            # Spec validation - this also filters irrelevant attributes, keeping only what is in the spec.
            # If an entry is not up to spec, this will raise a ValidationError.
            try:
                entry = self.cv(**attrs)
                row = self.aslist(entry)
                table.append(row)
            except ValidationError as err:
                print(err, attrs)



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
