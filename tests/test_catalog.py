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

    os.makedirs("tests/test_data/catalog/tds", exist_ok=True)

    for key in CATALOG_TDS_PATH.keys():
        url = TDS_ROOT + CATALOG_TDS_PATH[key] + "/catalog.xml"
        for xml in tds.walk(url, max_iterations=1):
            with open(f"tests/test_data/catalog/tds/{key}.ncml", "wb") as fh:
                fh.write(xml)


def read_ncml(name):
    """Return test xml."""
    return open(f"test_data/catalog/tds/{name}.ncml", "rb").read()


def get_elem(name):
    """element node parsed from the XML for the given data model."""
    from lxml.etree import XMLParser, fromstring

    xml = read_ncml(name)
    parser = XMLParser(encoding='UTF-8')
    return fromstring(xml, parser=parser)


@pytest.mark.parametrize("collection", collections)
def test_datamodel(collection):
    """Test attributes parsing and ingestion."""
    # Get data model
    dm = REGISTRY[collection]

    # Load example XML node
    # Make sure you've run `refresh_testdata`.
    elem = get_elem(collection)

    # Parse the attributes
    dm.from_orm(elem)


@pytest.mark.parametrize("collection", collections)
def test_intake_converter(collection):
    """Test catalog creation."""
    dm = REGISTRY[collection]
    ncml = read_ncml(collection)

    esmcat = Intake(cv=dm)
    esmcat.catalog([ncml])

