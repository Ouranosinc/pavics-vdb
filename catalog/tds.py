"""Utility function to parse metadata from a THREDDS Data Server catalog."""
from functools import lru_cache

import requests
from loguru import logger
from siphon.catalog import TDSCatalog
from xml.etree.ElementTree import ParseError, Element
from typing import Iterable


def _walk(cat, depth: int = 1, limit: int = None):
    """Generator walking a THREDDS data catalog for datasets.

    Parameters
    ----------
    cat : TDSCatalog
      THREDDS catalog.
    depth : int
      Maximum recursive depth. Setting 0 will return only datasets within the top-level catalog. If None,
      depth is set to 1000.
    limit : int
      Maximum number of datasets per catalog to walk through. Default is to go through all datasets.
    """
    for i, (name, ds) in enumerate(cat.datasets.items()):
        if limit and i == limit:
            break
        yield cat, name, ds

    if depth is None:
        depth = 1000

    if depth > 0:
        for name, ref in cat.catalog_refs.items():
            try:
                child = ref.follow()
                yield from _walk(child, depth=depth - 1, limit=limit)

            except requests.HTTPError as exc:
                logger.warning(exc)




def walk(url: str, max_iterations: int = 1E6, limit: int = None) -> Iterable[bytes]:
    """Return generator walking through a THREDDS Data Server catalog, and yield NcML file content.

    Parameters
    ----------
    url : str
      Thredds catalog url.
    max_iterations : int
      Maximum number of values returned by iterator.
    limit : int
      Maximum number of datasets per catalog.

    Returns
    -------
    Element
      <ncml:netcdf> element.
    """
    logger.info(f"Walking {url}")

    try:
        catalog = TDSCatalog(url)

    except ParseError as err:
        raise ConnectionError(f"Could not open {url}\n") from err

    for i, (cat, name, ds) in enumerate(_walk(catalog, depth=None, limit=limit)):
        if i >= max_iterations:
            return
        ncml_url = ds.access_urls["NCML"]
        yield get_ncml(ncml_url, catalog=cat.catalog_url, dataset=ds.url_path)


@lru_cache(512)
def get_ncml(url: str, catalog: str, dataset: str) -> bytes:
    """Read NcML response from server.

    Parameters
    ----------
    url : str
      Link to NcML service of dataset hosted on a THREDDS server.
    catalog : str
      Link to catalog storing the dataset.
    dataset : str
      Relative link to the dataset.

    Returns
    -------
    bytes
      NcML content
    """
    import requests

    # Setting `params` reproduces the NcML response we get when we click on the NcML service on THREDDS.
    # For some reason, this is required to obtain the "THREDDSMetadata" group and the available services.
    # Note that the OPENDAP link would have been available from the top "location" attribute.
    r = requests.get(url, params={"catalog": catalog, "dataset": dataset})
    logger.info(r.url)
    return r.content
