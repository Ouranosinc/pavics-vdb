import xncml
import os
import glob
import numpy as np
from distutils.dir_util import copy_tree
import threddsclient
import xarray as xr
import shutil
import pytest

# local path root to test data
local_root = './test_data/'
# thredds path root for data transfer
thredds_root = '/home/travis/boreas/testdata/NcML_tests/'
# correponding url to `thredds_root`
thredds_cat_root = 'https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/birdhouse/testdata/NcML_tests/'

class TestTimechanging:
    test_folder = ['TimeChanging', 'TimeConstant']
    # local path
    outpath = os.path.join(local_root,'ncdata_test_changing_time')
    # path to disk on thredds server
    thredds_path = os.path.join(thredds_root,'TestTimeChanging')
    # url to `thredds_path` catalog
    thredds_path_server = f'{thredds_cat_root}/TestTimeChanging/catalog.html'

    if not os.path.exists(thredds_path):
        os.makedirs(thredds_path)
    shutil.rmtree(thredds_path)
    for t in test_folder:
        nc = xncml.Dataset('./test_data/NcML_templates/NcML_template.ncml')
        nc.ncroot['netcdf']['aggregation']['scan']['@location'] = t
        if t == 'TimeChanging':
            # Use time units change flag
            nc.ncroot['netcdf']['aggregation']['@timeUnitsChange'] = "true"

        nc.to_ncml(os.path.join(outpath, f'{t}-agg-NcML.ncml'))

    copy_tree(outpath, thredds_path)
    datasets = [d.opendap_url() for d in threddsclient.crawl(thredds_path_server, depth=2) if '.ncml' in d.name]
    ds_all = {}

    for d in datasets:
        if 'TimeConstant' in d:
            type1 = 'TimeConstant'
        elif 'TimeChanging' in d:
            type1 = 'TimeChanging'

        ds_all[type1] = dict(ncml=xr.open_dataset(d))
        ds_all[type1]['mf_ds'] = xr.open_mfdataset(sorted(glob.glob(os.path.join(outpath, f'{type1}/*.nc'))),
                                                   combine='by_coords')
    def test_timeconstantNCML_vs_timeconstantMF_DS(self):
        type1 = 'TimeConstant'
        # Time Constant NcML vs Time Constant MF - Dataset
        for v in ['tasmin','time']:
            np.testing.assert_array_equal(self.ds_all[type1]['ncml'][v], self.ds_all[type1]['mf_ds'][v])

    def test_timeconstantNCML_vs_timechangingMF_DS(self):
        # Time Constant NcML vs Time Constant MF - Dataset
        for v in ['tasmin', 'time']:
            np.testing.assert_array_equal(self.ds_all['TimeConstant']['ncml'][v], self.ds_all['TimeChanging']['mf_ds'][v])

    def test_timechangingNCML_vs_timechangingMF_DS(self):
        type1 = 'TimeChanging'
        # Time Constant NcML vs Time Constant MF - Dataset
        for v in ['tasmin', 'time']:
            np.testing.assert_array_equal(self.ds_all[type1]['ncml'][v], self.ds_all[type1]['mf_ds'][v])

