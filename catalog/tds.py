"""Utility function to parse metadata from a THREDDS Data Server catalog."""
import requests
import logging

# setup logger
LOGGER = logging.getLogger(__name__)


def walk(cat, depth=1):
    """Return a generator walking a THREDDS data catalog for datasets.

    Parameters
    ----------
    cat : TDSCatalog
      THREDDS catalog.
    depth : int
      Maximum recursive depth. Setting 0 will return only datasets within the top-level catalog. If None,
      depth is set to 1000.
    """
    yield from cat.datasets.items()
    if depth is None:
        depth = 1000

    if depth > 0:
        for name, ref in cat.catalog_refs.items():
            try:
                child = ref.follow()
                yield from walk(child, depth=depth - 1)

            except requests.HTTPError as exc:
                LOGGER.warning(exc)



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
