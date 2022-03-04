import xarray as xr
from urlpath import URL

ROOT = URL("https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/dodsC")

class TestNcML:
    path = '.'
    
    def __init__(self):
        self.url = ROOT / self.path

    def test_exists(self):
       xr.open_dataset(self.url)
