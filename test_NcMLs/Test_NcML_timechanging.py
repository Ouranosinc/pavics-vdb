import xncml
import os
import glob
import numpy as np
from distutils.dir_util import copy_tree
import threddsclient
import xarray as xr
test_folder = ['TimeChanging','TimeConstant']
outpath  = './test_data/ncdata_test_changing_time'
thredds_path = '/home/travis/boreas/testdata/NcML_tests'
thredds_path_server = 'https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/birdhouse/testdata/NcML_tests/catalog.html'
if not os.path.exists(thredds_path):
    os.makedirs(thredds_path)
for t in test_folder:
    nc = xncml.Dataset('./test_data/NcML_templates/NcML_template.ncml')
    nc.ncroot['netcdf']['aggregation']['scan']['@location'] = t
    if t == 'TimeChanging':
        # Use time units change flag
        nc.ncroot['netcdf']['aggregation']['@timeUnitsChange'] = "true"

    nc.to_ncml(os.path.join(outpath,f'{t}-agg-NcML.ncml'))

copy_tree('./test_data/ncdata_test_changing_time', thredds_path)

datasets = [d.opendap_url() for d in threddsclient.crawl(thredds_path_server, depth=2) if '.ncml' in d.name]
# opendap
d0 = xr.open_dataset(datasets[0])
d0a  = xr.open_mfdataset(sorted(glob.glob('./test_data/ncdata_test_changing_time/TimeConstant/*.nc')), combine='nested', concat_dim='time')
#
d1 = xr.open_dataset(datasets[1])
d1a= xr.open_mfdataset(sorted(glob.glob('./test_data/ncdata_test_changing_time/TimeChanging/*.nc')), combine='nested', concat_dim='time')

# Time Constant NcML vs Time Constant MF - Dataset
np.testing.assert_array_equal(d0.tasmin,d0a.tasmin)
np.testing.assert_array_equal(d0.time,d0a.time)

# Time Constant NcML vs Time Changing MF - Dataset
np.testing.assert_array_equal(d0.tasmin,d1a.tasmin)
np.testing.assert_array_equal(d0.time,d1a.time)


np.testing.assert_array_equal(d0.tasmin,d1.tasmin)
print(np.unique(d1.time.values - d0.time.values))
np.testing.assert_array_equal(d0.time, d1.time)

