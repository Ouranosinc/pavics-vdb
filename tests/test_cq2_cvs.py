import pytest
from collections import defaultdict
from catalog.datamodels.cq2 import cvs, pat, reverse_format
from catalog.config import TDS_ROOT, CATALOG_TDS_PATH
from siphon.catalog import TDSCatalog


@pytest.mark.online
def test_cq2_cvs():
    """Test that the Data Reference Syntax for the file name is covered by the CV."""
    url = TDS_ROOT + CATALOG_TDS_PATH["cq2"] + "/catalog.xml"
    cat = TDSCatalog(url)

    ds = [reverse_format(pat, p) for p in cat.datasets]

    out = defaultdict(set)
    for d in ds:
        for k, v in d.items():
            out[k].add(v)

    for k, values in out.items():
        if k.startswith("_"):
            continue
        for v in values:
            assert v in cvs[k]
