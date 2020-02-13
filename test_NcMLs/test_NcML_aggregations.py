import xncml
import os
import glob
import numpy as np
from distutils.dir_util import copy_tree
import threddsclient
import xarray as xr
import shutil
from itertools import combinations
import pytest

home = os.environ['HOME']
pavics_root = os.path.join(home, 'boreas')  # mapped drive to top level `Birdhouse` folder on thredds

# local path root to test data
local_root = './test_data/'
# thredds path root for data transfer and thredds catalog url
if home == '/home/biner':
    # patch to deal with permission problems ...
    thredds_root = os.path.join(pavics_root, 'testdata/biner/NcML_tests/')
    thredds_cat_root = 'https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/birdhouse/testdata/biner/NcML_tests/'
else:
    thredds_root = os.path.join(pavics_root, 'testdata/NcML_tests/')
    # correponding url to `thredds_root`
    thredds_cat_root = 'https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/birdhouse/testdata/NcML_tests/'

def set_paths(test_name):
    """function setting local and thredds paths"""

    test_dir = 'Test'+test_name
    local_test_dir = os.path.join(local_root, 'ncdata_testNCML', test_dir)
    # hypothese : directory with nc data are in local_test_dir/test_name

    # set thredds paths
    thredds_test_dir = os.path.join(thredds_root, test_dir)
    if os.path.exists(thredds_test_dir):
        shutil.rmtree(thredds_test_dir)

    # delete local .ncmls
    for f in glob.glob(os.path.join(local_test_dir,'*.ncml')):
        os.remove(f)

    # clean thredds directory
    if os.path.exists(thredds_test_dir):
        shutil.rmtree(thredds_test_dir)

    return local_test_dir, thredds_test_dir

class TestTimechanging:
    """tests what happens when time units varies between nc files"""

    # set the paths
    test_name = 'TimeChanging'
    local_test_dir, thredds_test_dir = set_paths(test_name)

    # make new ncml files
    nc_folders = ['TimeChanging', 'TimeConstant']
    for t in nc_folders:
        nc = xncml.Dataset('./test_data/NcML_templates/NcML_template.ncml')
        nc.ncroot['netcdf']['aggregation']['scan']['@location'] = t
        if t == 'TimeChanging':
            # Use time units change flag
            nc.ncroot['netcdf']['aggregation']['@timeUnitsChange'] = "true"
        nc.to_ncml(os.path.join(local_test_dir, f'{t}-agg-NcML.ncml'))

    # copy local nc and ncml files on the thredds server
    copy_tree(local_test_dir, thredds_test_dir)

    # url to `thredds_path` catalog
    thredds_path_server = f'{thredds_cat_root}/Test{test_name}/catalog.html'
    datasets = [d.opendap_url() for d in threddsclient.crawl(thredds_path_server, depth=0) if '.ncml' in d.name]
    ds_all = {}

    for d in datasets:
        if 'TimeConstant' in d:
            type1 = 'TimeConstant'
        elif 'TimeChanging' in d:
            type1 = 'TimeChanging'

        ds_all[type1] = dict(ncml=xr.open_dataset(d))
        ds_all[type1]['mf_ds'] = xr.open_mfdataset(sorted(glob.glob(os.path.join(local_test_dir, f'{type1}/*.nc'))),
                                                   combine='by_coords')
    def test_timeconstantNCML_vs_timeconstantMF_DS(self):
        type1 = 'TimeConstant'
        # Time Constant NcML vs Time Constant MF - Dataset
        for v in ['tasmin','time']:
            np.testing.assert_array_equal(self.ds_all[type1]['ncml'][v], self.ds_all[type1]['mf_ds'][v])

    #@pytest.mark.xfail
    def test_timeconstantNCML_vs_timechangingMF_DS(self):
        # Time Constant NcML vs Time Constant MF - Dataset
        for v in ['tasmin', 'time']:
            np.testing.assert_array_equal(self.ds_all['TimeConstant']['ncml'][v], self.ds_all['TimeChanging']['mf_ds'][v])

    @pytest.mark.xfail
    def test_timechangingNCML_vs_timechangingMF_DS(self):
        type1 = 'TimeChanging'
        # Time Constant NcML vs Time Constant MF - Dataset
        for v in ['tasmin', 'time']:
            np.testing.assert_array_equal(self.ds_all[type1]['ncml'][v], self.ds_all[type1]['mf_ds'][v])

class TestAbsolutepaths:
    """verify that ncml is not sensible to location when using absolute paths"""

    # set the paths
    test_name = 'AbsolutePath'
    local_test_dir, thredds_test_dir = set_paths(test_name)

    # make the ncml file
    ncml_file = os.path.join(local_test_dir, f'AbsolutePath-agg-NcML.ncml')
    nc = xncml.Dataset('./test_data/NcML_templates/NcML_template.ncml')
    ncfiles_location = os.path.join(thredds_test_dir.replace(pavics_root, '/pavics-data/'), 'ncfiles')
    nc.ncroot['netcdf']['aggregation']['scan']['@location'] = ncfiles_location
    nc.to_ncml(ncml_file)

    # copy local nc and ncml files on the thredds server
    copy_tree(local_test_dir, thredds_test_dir)

    # copy .ncml elsewhere on thredds - Should still work
    shutil.copy(ncml_file, thredds_root)

    # url to `thredds_path` catalog
    thredds_path_server = f'{thredds_cat_root}/Test{test_name}/catalog.html'

    datasets = [d.opendap_url() for d in threddsclient.crawl(thredds_path_server, depth=0) if '.ncml' in d.name]
    datasets1 = [d.opendap_url() for d in threddsclient.crawl(thredds_cat_root, depth=0) if '.ncml' in d.name]
    assert len(datasets) == 1
    assert len(datasets1) == 1

    ds_all = dict(ncml1=xr.open_dataset(datasets[0]))
    ds_all['ncml2'] = xr.open_dataset(datasets1[0])
    local_ncfiles = sorted(glob.glob(os.path.join(local_test_dir, f'ncfiles/*.nc')))
    ds_all['mf_ds'] = xr.open_mfdataset(local_ncfiles, combine='by_coords')

    def test_AbsPathNCML(self):
        combs = combinations(self.ds_all.keys(), 2)
        for c in list(combs):
            for v in ['tasmin','time']:
                np.testing.assert_array_equal(self.ds_all[c[0]][v],  self.ds_all[c[1]][v])

class TestVaryingAttributes:
    """testing what happens with ncml aggregation with files having different attributes values"""

    # set the paths
    test_name = 'VaryingAttributes'
    local_test_dir, thredds_test_dir = set_paths(test_name)

    # generate local ncml file
    nc = xncml.Dataset('./test_data/NcML_templates/NcML_template.ncml')
    ncmlfile = 'VaryingAttributes-agg-NCML.ncml'
    nc.to_ncml(os.path.join(local_test_dir, ncmlfile))
    thredds_location = os.path.join(thredds_test_dir.replace(pavics_root, '/pavics-data'), 'ncfiles')
    nc.ncroot['netcdf']['aggregation']['scan']['@location'] = thredds_location
    nc.to_ncml(os.path.join(local_test_dir, ncmlfile))

    # copy local_test_dir to thredds server
    copy_tree(local_test_dir, thredds_test_dir)

    # url to `thredds_path` catalog
    thredds_path_server = f'{thredds_cat_root}/Test{test_name}/catalog.html'

    # open ncml dataset with xarray
    dataset = [d.opendap_url() for d in threddsclient.crawl(thredds_path_server, depth=0) if '.ncml' in d.name]
    assert(len(dataset)==1)
    ds_ncml = xr.open_dataset(dataset[0])

    def test_aggregated_attributes_equal_before_last_file_attributes(self):
        clef = os.path.join(self.thredds_test_dir, 'ncfiles', '*.nc')
        l_f = sorted(glob.glob(clef))
        ds_before_last = xr.open_dataset(l_f[-2])
        assert(self.ds_ncml.attrs == ds_before_last.attrs)

class TestRecursiveAggregation:
    """testing aggregation of ncml files"""

    # set paths
    test_name = 'RecursiveAggregation'
    local_test_dir, thredds_test_dir = set_paths(test_name)

    # generate local ncml file
    nc = xncml.Dataset('./test_data/NcML_templates/NcML_template.ncml')
    ncmldir = os.path.join(local_test_dir, 'ncmlfiles')
    if not os.path.exists(ncmldir):
        os.makedirs(ncmldir)
    # tasmax
    nc.ncroot['netcdf']['aggregation']['scan']['@location']='../ncfiles/tasmax'
    nc.add_dataset_attribute(key='ncml_origin', type_='string', value='tasmax')
    ncmlfile = 'tasmax-agg-NcML.ncml'
    nc.to_ncml(os.path.join(ncmldir, ncmlfile))
    # tasmin
    nc.ncroot['netcdf']['aggregation']['scan']['@location']='../ncfiles/tasmin'
    nc.ncroot['netcdf']['attribute']['@value'] = 'tasmin'
    ncmlfile = 'tasmin-agg-NcML.ncml'
    nc.to_ncml(os.path.join(ncmldir, ncmlfile))
    # aggregation of tasmin and tasmax ncml files
    nc.ncroot['netcdf']['attribute']['@value'] = 'ncml_aggregation'
    del nc.ncroot['netcdf']['aggregation']['@dimName']
    nc.ncroot['netcdf']['aggregation']['@type'] = 'union'
    nc.ncroot['netcdf']['aggregation']['scan']['@location'] = 'ncmlfiles'
    nc.ncroot['netcdf']['aggregation']['scan']['@suffix'] = '.ncml'
    nc.to_ncml(os.path.join(local_test_dir, 'recursive-agg-NcML.ncml'))

    # copy local_test_dir to thredds server
    copy_tree(local_test_dir, thredds_test_dir)

    # open aggregated dataset
    thredds_path_server = f'{thredds_cat_root}/Test{test_name}/catalog.html'
    dataset = [d.opendap_url() for d in threddsclient.crawl(thredds_path_server, depth=0) if '.ncml' in d.name]
    assert(len(dataset)==1)
    ds_agg = xr.open_dataset(dataset[0])

    # open tasmin and tasmax datasets
    thredds_path_server = f'{thredds_cat_root}/Test{test_name}/ncmlfiles/catalog.html'
    dataset = [d.opendap_url() for d in threddsclient.crawl(thredds_path_server, depth=0) if '.ncml' in d.name]
    ds_tasmin = xr.open_dataset(dataset[0])
    ds_tasmax = xr.open_dataset(dataset[1])

    def test_recursive_aggregation(self):
        np.testing.assert_array_equal(ds_agg.tasmax[:], ds_tasmax.tasmax[:])
        np.testing.assert_array_equal(ds_agg.tasmin[:], ds_tasmin.tasmin[:])

    def test_aggregated_attributes(self):
        assert(ds_agg.attrs['ncml_origin']=='ncml_aggregation')
        assert (ds_tasmin.attrs['ncml_origin'] == 'tasmin')
        assert (ds_tasmax.attrs['ncml_origin'] == 'tasmax')




