import pytest
from catalog.datamodels import REGISTRY
from catalog.intake_ingestion.intake_converter import Intake
from catalog.ncml import to_element
import xncml
collections = list(REGISTRY.keys())
import intake

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


def walk_test_collection(name):
    """Element node parsed from the XML for the given data model.

    Parameters
    ----------
    name: str
      Data model name.
    """
    from pathlib import Path

    data_dir = Path(f"test_data/catalog/tds/")
    if not data_dir.exists():
        raise IOError(Path(".").absolute())
    fns = data_dir.glob(f"{name}*")
    for fn in fns:
        yield fn


@pytest.mark.parametrize("collection", collections)
def test_datamodel(collection):
    """Test attributes parsing and ingestion."""

    dm = REGISTRY[collection]
    for fn in walk_test_collection(collection):
        attrs = xncml.Dataset(fn).to_cf_dict()
        dm(**attrs)


@pytest.mark.parametrize("collection", collections)
def test_intake_converter(collection, tmp_path):
    """Test catalog creation."""
    dm = REGISTRY[collection]
    esmcat = Intake(cv=dm)

    for fn in walk_test_collection(collection):
        ncml = fn.read_bytes()
        cat = esmcat.catalog([[collection, ncml],])
        out_fn = esmcat.save(cat, path=tmp_path, name=collection)
        intake.open_esm_datastore(out_fn)


