import pytest
from catalog.datamodels import REGISTRY
from catalog.intake_ingestion.intake_converter import Intake
from catalog.ncml import to_element

collections = list(REGISTRY.keys())


def refresh_testdata():
    """For each catalog data model, fetch an NcML file from the server and save on disk.
    This is useful to run offline tests.
    """
    import os
    from catalog.config import TDS_ROOT, CATALOG_TDS_PATH
    from catalog import tds

    os.makedirs("tests/test_data/catalog/tds", exist_ok=True)

    for key, paths in CATALOG_TDS_PATH.items():
        for path, dm in paths.items():
            url = TDS_ROOT + path + "/catalog.xml"
            for name, xml in tds.walk(url, limit=1):
                fn = f"tests/test_data/catalog/tds/{dm}_{name}"
                with open(fn, "wb") as fh:
                    fh.write(xml)


def get_elems(name):
    """Element node parsed from the XML for the given data model.

    Parameters
    ----------
    name: str
      Data model name.
    """
    from lxml.etree import XMLParser, fromstring
    from pathlib import Path

    data_dir = Path(f"test_data/catalog/tds/")
    if not data_dir.exists():
        raise IOError(Path(".").absolute())
    fns = data_dir.glob(f"{name}*")
    for fn in fns:
        with open(fn, "rb") as xml:
            dm = fn.stem.split("_")[0]
            yield fn.name, dm, to_element(xml.read())


@pytest.mark.parametrize("collection", collections)
def test_datamodel(collection):
    """Test attributes parsing and ingestion."""
    from pydantic import ValidationError

    # Load example XML node
    # Make sure you've run `refresh_testdata`.
    for fn, dm_name, elem in get_elems(collection):

        # Get data model
        dm = REGISTRY[dm_name]
        # Parse the attributes
        try:
            dm.from_orm(elem)
        except ValidationError as exc:
            raise ValueError(f"{fn}: \n{exc}")



def test_meta():
    from catalog.datamodels.biasadjusted import BiasAjustedMeta
    from catalog.ncml import to_element

    for fn, dm_name, elem in get_elems("biasadjusted"):
        BiasAjustedMeta.from_orm(elem)


@pytest.mark.parametrize("collection", collections)
def test_intake_converter(collection):
    """Test catalog creation."""
    dm = REGISTRY[collection]
    ncml = read_ncml(collection)

    esmcat = Intake(cv=dm)
    esmcat.catalog([ncml])

