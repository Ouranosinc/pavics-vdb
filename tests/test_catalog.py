import pytest
from catalog.datamodels import REGISTRY
from catalog.intake_ingestion.intake_converter import Intake


collections = list(REGISTRY.keys())


def refresh_testdata():
    """For each catalog data model, fetch an NcML file from the server and save on disk.
    This is useful to run offline tests.
    """
    import os
    from catalog.config import TDS_ROOT, CATALOG_TDS_PATH
    from catalog import tds

    os.makedirs("test_data/catalog/tds", exist_ok=True)

    for key, paths in CATALOG_TDS_PATH.items():
        for path, dm in paths.items():
            url = TDS_ROOT + path + "/catalog.xml"
            for i, xml in enumerate(tds.walk(url, limit=1)):
                with open(f"test_data/catalog/tds/{key}_{dm}_{i}.ncml", "wb") as fh:
                    fh.write(xml)


def get_elems(name):
    """element node parsed from the XML for the given data model."""
    from lxml.etree import XMLParser, fromstring
    from pathlib import Path

    fns = Path(f"test_data/catalog/tds/").glob(f"{name}*")
    parser = XMLParser(encoding='UTF-8')

    for fn in fns:
        with open(fn, "rb") as xml:
            col, dm, i = fn.stem.split("_")
            yield fn.name, dm, fromstring(xml.read(), parser=parser)


@pytest.mark.parametrize("collection", ["biasadjusted"]) #collections
def test_datamodel(collection):
    """Test attributes parsing and ingestion."""

    # Load example XML node
    # Make sure you've run `refresh_testdata`.
    for fn, dm_name, elem in get_elems(collection):
        # Get data model
        dm = REGISTRY[dm_name]
        # Parse the attributes
        dm.from_orm(elem)


@pytest.mark.parametrize("collection", collections)
def test_intake_converter(collection):
    """Test catalog creation."""
    dm = REGISTRY[collection]
    ncml = read_ncml(collection)

    esmcat = Intake(cv=dm)
    esmcat.catalog([ncml])

