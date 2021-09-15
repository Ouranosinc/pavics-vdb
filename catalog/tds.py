"""Utility function to parse metadata from a THREDDS Data Server catalog."""
import subprocess
from pathlib import Path


def walk_tds(cat, depth=1):
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
            child = ref.follow()
            yield from walk_tds(child, depth=depth-1)


def walk_fs(paths):
    """Return a generator walking netCDF files under paths.

    Parameters
    ----------
    paths : list
      Absolute paths on the filesystem.
    """
    def _file_dir_files(directory):
        """Return list of netCDF files within directory."""
        try:
            cmd = ["find", "-L", directory.as_posix(), "-name", "*.nc"]
            proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            output = proc.stdout.read().decode("utf-8").split()

        except Exception:
            output = []
        return output

    for r in paths:
        for file in _file_dir_files(Path(r)):
            yield file


def walk_tds_ncml(cat, depth=1):
    """Return attributes from TDS NcML service for all datasets in catalog."""
    for name, ds in walk_tds(cat, depth):
        yield attrs_from_ds(ds)


def walk_disk_ncml(paths):
    """Return attributes from all netCDF files under file system paths."""
    def _ncdump(path):
        cmd = ["ncdump", "-hx", path.as_posix()]
        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        return proc.stdout.read()

    for path in walk_fs(paths):
        attrs = attrs_from_ncml(_ncdump(Path(path)))
        attrs["__services__"] = {"FS": path}
        yield attrs


def attrs_from_ds(ds):
    """Extract attributes from TDS Dataset."""
    import requests

    url = ds.access_urls["NCML"]
    xml = requests.get(url).content
    attrs = attrs_from_ncml(xml)
    attrs["__services__"] = ds.access_urls
    return attrs


def attrs_from_ncml(xml):
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
