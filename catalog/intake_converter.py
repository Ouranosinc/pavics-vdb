"""Create intake catalog from NcML attributes.

# Overview

- Given a THREDDS location, walk datasets and subdirectories
- For each dataset, request NcML service output -> xml
- Convert xml into dictionary of attributes (extract_attrs_from_ncml)
- Configure attributes that should go into catalog (todo : ask Travis)
- Convert list of dicts to csv table using config (todo)
- Create intake json entry linking to csv table and holding column metadata (todo, unclear how to configure)

"""

from siphon.catalog import TDSCatalog

# Catalog content
spec = {
    "esmcat_version": "0.1.0",
    "id": "test",
    "description": "test catalog",
    "catalog_file": "pavics_datasets.csv",
    "attributes": [
        {"column_name": "project_id"},
        {"column_name": "institute_id"},
        {"column_name": "type"},
        {"column_name": "driving_experiment"},
        {"column_name": "driving_model"},
        {"column_name": "processing"},
        {"column_name": "license_type"}
    ],
    "assets":{
        "column_name": "netcdf",
        "format": "netcdf"
    }
}

# Dictionary of facet names, keyed by the standard name that will appear in catalog
alternative_facet_names = {}


def build_catalog(cat):
    keys = [att["column_name"] for att in spec["attributes"]]

    header = keys + [spec["assets"]["column_name"]]
    table = [header]
    for name, gl, gr, va in walk_extract(cat):
        row = [gl.get(k, "NA") for k in keys]
        row.append(gr["services"]["OPENDAP"])
        table.append(row)
    return table


def write_catalog(table):
    import csv
    import json

    # Write spec
    with open("pavics.json", "w") as f:
        json.dump(spec, f)

    fn = spec["catalog_file"]
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


def walk_extract(cat, depth=1):
    out = {}
    for name, ds in walk(cat):
        url = ds.access_urls["NCML"]
        gl, gr, va = extract_attrs_from_ncml(url)
        gr["services"] = ds.access_urls
        yield name, gl, gr, va


def extract_attrs_from_ncml(url):
    """Extract attributes from NcML file.

    Parameters
    ----------
    url : str
      Link to NcML service of THREDDS server for a dataset.
    which : {'global', 'variable',
    facets : tuple
      Attribute names.

    Returns
    -------
    dict
      Attribute values keyed by facet names.
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
    gl = attrib_to_dict(nc.xpath("ncml:attribute", namespaces=ns))

    # Extract group attributes
    gr = {}
    for group in nc.xpath("ncml:group", namespaces=ns):
        gr[group.attrib["name"]] = attrib_to_dict(group.xpath("ncml:attribute", namespaces=ns))

    # Extract variable attributes
    va = {}
    for variable in nc.xpath("ncml:variable", namespaces=ns):
        if '_CoordinateAxisType' in variable.xpath("ncml:attribute/@name", namespaces=ns):
            continue
        va[variable.attrib["name"]] = attrib_to_dict(variable.xpath("ncml:attribute", namespaces=ns))

    return gl, gr, va


def attrib_to_dict(elems):
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
