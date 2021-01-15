"""Create intake catalog from NcML attributes.

# Overview

- Given a THREDDS location, walk datasets and subdirectories (todo : easy using siphon.catalog.TDSCatalog)
- For each dataset, request NcML service output -> xml (todo : easy using ds.access_urls["NCML"])
- Convert xml into dictionary of attributes (extract_attrs_from_ncml)
- Configure attributes that should go into catalog (todo : ask Travis)
- Convert list of dicts to csv table using config (todo)
- Create intake json entry linking to csv table and holding column metadata (todo, unclear how to configure)

"""

# Catalog content
facets = ()

# Dictionary of facet names, keyed by the standard name that will appear in catalog
alternative_facet_names = {}


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
    ns = {"ncml": "http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"}

    # Parse XML content
    xml = requests.get(url).text
    doc = lxml.etree.parse(xml)
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
