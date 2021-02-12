"""Create intake catalog from NcML attributes.

See https://github.com/DACCS-Climate/roadmap/wiki/Catalog-design-and-architecture

requirements = siphon intake intake-esm lxml


# Example

table = build_catalog(Reanalysis)
write_catalog(Reanalysis, table)

import intake
cat = intake.open_esm_datastore("reanalysis.json")
cat.df


# Overview

- Given a THREDDS location, walk datasets and subdirectories
- For each dataset, request NcML service output -> xml
- Convert xml into dictionary of attributes (extract_attrs_from_ncml)
- Configure attributes that should go into catalog (todo : ask Travis)
- Convert list of dicts to csv table using config
- Create intake json entry linking to csv table and holding column metadata

# Notes

https://intake-esm.readthedocs.io/en/latest/user-guide/multi-variable-assets.html

Gab:
- grid resolution (degree)
- standard Not Applicable value
- institude_id (gcm/rcm)
- how-to ( for first member, highest-resolution)

# Open questions

- One catalog for everything, or a catalog for reanalyses, simulations, etc ?
- Variable coded with CMIP output variable name ? (only standard_name is in CF convention)
   * Can use synonym table from cf-index-meta

"""
from collections import defaultdict
from siphon.catalog import TDSCatalog
from xml.etree.ElementTree import ParseError
from dataclasses import dataclass, fields, asdict, astuple
import tds

ESMCAT_VERSION = "0.1.0"


class Intake:
    """Class that, combined with a DRS class, facilitates the creation of Intake catalogs."""
    asset_attribute = "path"
    asset_format = "netcdf"

    # Attributes that should be standard but may not be available everywhere.
    # version: str
    # variable_long_name: str
    # variable_standard_name: str
    # variable_units: str
    # time_coverage_start: dt.datetime
    # time_coverage_end: dt.datetime

    # Not implemented yet, but would include relationships to other datasets.
    # provenance: str = ""

    def __init__(self, drs):
        self.drs = drs

    def to_intake_spec(self):
        """Create Intake specification file."""

        attributes = [{"column_name": key} for key in self.drs.global_attributes() + self.drs.variable_attributes()]
        spec = {"esmcat_version": ESMCAT_VERSION,
                "id": self.drs.__name__.lower(),
                "description": self.drs.__doc__.splitlines()[0],
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
        return self.drs.__name__.lower()

    @property
    def catalog_fn(self):
        """Return catalog file name."""
        return f"{self.cid}.csv.gz"

    def header(self):
        """Return attribute table header names."""
        return self.drs.global_attributes() + self.drs.variable_attributes() + [self.asset_attribute]

    def validate(self, **kwargs):
        return self.drs(**kwargs)

    def aslist(self, entry, asset):
        """Return tuple of values."""
        d = asdict(entry)
        d[self.asset_attribute] = asset
        return [d[k] for k in self.header()]

    def parse(self, url):
        """Return a table including a header and rows of attribute values based on Data Reference Syntax.
        """
        try:
            cat = TDSCatalog(url)
        except ParseError as err:
            raise ConnectionError(f"Could not open {url}\n") from err

        table = [self.header()]
        for name, ds in tds.walk(cat):
            attrs = tds.attrs_from_ds(ds)

            # Global attributes
            g_val = {k: attrs.get(k, "NA") for k in self.drs.global_attributes()}

            # Variable attributes (lists)
            v_val = defaultdict(list)
            for key in self.drs.variable_attributes():
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

        with gzip.open(filename=path / self.catalog_fn, mode="wt") as f:
            w = csv.writer(f)
            for row in table:
                w.writerow(row)
