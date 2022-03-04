"""git clone --depth=1 git@github.com:Unidata/netcdf-java.git"""
from pathlib import Path
# TODO: Configure the path to the netcdf-java directory


TESTDIR = ROOT / "cdm" / "src" / "test" / "data" / "ncml"


def test_dimensions(ds):

