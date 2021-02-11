"""Create intake catalog from NcML attributes.

See https://github.com/DACCS-Climate/roadmap/wiki/Catalog-design-and-architecture


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
from dataclasses import fields
from collections import defaultdict
from siphon.catalog import TDSCatalog
from xml.etree.ElementTree import ParseError
from specs import CMIP5, BiasAdjusted, Reanalysis, GridObs, Forecast

TDS_ROOT = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/datasets/"

# Mapping of DRS classes to TDS paths
CATALOG_PATH = {CMIP5: "simulations/cmip5_multirun",
                BiasAdjusted: "simulations/bias_adjusted",
                Reanalysis: "reanalyses",
                GridObs: "gridded_obs",
                Forecast: "forecasts"}

# Dictionary of facet names, keyed by the standard name that will appear in catalog
alternative_facet_names = {}


def build_table(spec):
    """Return a table including a header and rows of attribute values based on Data Reference Syntax.
    """
    url = TDS_ROOT + CATALOG_PATH[spec] + "/catalog.xml"
    try:
        cat = TDSCatalog(url)
    except ParseError as err:
        raise ConnectionError(f"Could not open {url}\n") from err

    table = [spec.header()]
    for name, ds in walk(cat):
        attrs = attrs_from_ds(ds)

        # Global attributes
        g_vals = {k: attrs.get(k, "NA") for k in spec.global_attributes()}

        # Asset path
        g_vals[spec.asset_attribute()] = attrs["__services__"]["OPENDAP"]

        # Variable attributes (lists)
        v_vals = defaultdict(list)
        for key in spec.variable_attributes():
            k = key.split("variable_")[1]
            for vk, vd in attrs["__variable__"].items():
                if k in vd:
                    v_vals[key].append(vd[k])
                elif k == "name":
                    v_vals[key].append(vk)
                else:
                    v_vals[key].append("NA")

        # Spec validation
        entry = spec(**g_vals, **v_vals)

        table.append(entry.aslist())

    return table


def write_catalog(spec, table):
    import csv
    import json

    # Write spec
    with open(f"{spec.cid()}.json", "w") as f:
        json.dump(spec.to_intake_spec(), f)

    fn = spec.catalog_fn()
    with open(fn, "w") as f:
        w = csv.writer(f)
        for row in table:
            w.writerow(row)


def walk(cat, depth=1):
    """Return a generator walking a THREDDS data catalog for datasets.

    Parameters
    ----------
    cat : TDSCatalog
      THREDDS catalog.
    depth : int
      Maximum recursive depth. Setting 0 will return only datasets within the top-level catalog.
    """
    yield from cat.datasets.items()

    if depth > 0:
        for name, ref in cat.catalog_refs.items():
            child = ref.follow()
            yield from walk(child, depth=depth-1)


def attrs_from_ds(ds):
    """Extract attributes from TDS Dataset."""
    url = ds.access_urls["NCML"]
    attrs = attrs_from_ncml(url)
    attrs["__services__"] = ds.access_urls
    return attrs


def attrs_from_ncml(url):
    """Extract attributes from NcML file.

    Parameters
    ----------
    url : str
      Link to NcML service of THREDDS server for a dataset.

    Returns
    -------
    dict
      Global attribute values keyed by facet names, with variable attributes in `__variable__` nested dict, and
      additional specialized attributes in `__group__` nested dict.
    """
    import lxml.etree
    import requests
    parser = lxml.etree.XMLParser(encoding='UTF-8')

    ns = {"ncml": "http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"}

    # Parse XML content - UTF-8 encoded documents need to be read as bytes
    xml = requests.get(url).content
    doc = lxml.etree.fromstring(xml, parser=parser)
    nc = doc.xpath("/ncml:netcdf", namespaces=ns)[0]

    # Extract global attributes
    out = _attrib_to_dict(nc.xpath("ncml:attribute", namespaces=ns))

    # Extract group attributes
    gr = {}
    for group in nc.xpath("ncml:group", namespaces=ns):
        gr[group.attrib["name"]] = _attrib_to_dict(group.xpath("ncml:attribute", namespaces=ns))

    # Extract variable attributes
    va = {}
    for variable in nc.xpath("ncml:variable", namespaces=ns):
        if '_CoordinateAxisType' in variable.xpath("ncml:attribute/@name", namespaces=ns):
            continue
        va[variable.attrib["name"]] = _attrib_to_dict(variable.xpath("ncml:attribute", namespaces=ns))

    out["__group__"] = gr
    out["__variable__"] = va

    return out


def _attrib_to_dict(elems):
    """Convert element attributes to dictionary.

    Ignore attributes with names starting with _
    """
    hidden_prefix = "_"
    out = {}
    for e in elems:
        a = e.attrib
        if a["name"].startswith(hidden_prefix):
            continue
        out[a["name"]] = a["value"]
    return out
