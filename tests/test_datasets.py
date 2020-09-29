import collections
import glob
import os
import pathlib as path
import shutil
from dask.diagnostics import ProgressBar
import numpy as np
import threddsclient
import xarray as xr
import xncml
from xclim import subset, ensembles
import netCDF4

from lxml import etree




home = os.environ['HOME']
pavics_root = os.path.join(home, 'boreas')  # mapped drive to top level `Birdhouse` folder on thredds
test_reg = dict(lon=[-80, -70], lat=[45, 50])
# local path root to test data
local_root = './test_data/'
# thredds path root for data transfer and thredds catalog url
if home == '/home/biner':
    # patch to deal with permission problems ...
    thredds_root = os.path.join(pavics_root, 'testdata/biner/NcML_tests/')
    thredds_cat_root = 'https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/birdhouse/testdata/biner/NcML_tests/'
else:
    pavics_root = os.path.join(home, 'boreas/boreas')
    thredds_root = os.path.join(home, 'boreas', 'testdatasets/')
    # correponding url to `thredds_root`
    thredds_cat_root = 'https://pavics.ouranos.ca/testthredds/testdatasets'
    #thredds_cat_root = 'https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/birdhouse/testdata/NcML_tests/'

class Validator:

    def __init__(self, xsd_path: str):
        xmlschema_doc = etree.parse(xsd_path)
        self.xmlschema = etree.XMLSchema(xmlschema_doc)

    def validate(self, xml_path: str) -> bool:
        xml_doc = etree.parse(xml_path)
        result = self.xmlschema.validate(xml_doc)

        return result

def recursive_items(dictionary, pattern):
    for key, value in dictionary.items():
        if key == pattern or value == pattern:
            yield (key, value)
            if type(value) is collections.OrderedDict:
                yield from recursive_items(value, pattern=pattern)
            elif type(value) is list:
                for l in value:
                    yield from recursive_items(l, pattern=pattern)
        else:
            if type(value) is collections.OrderedDict:
                yield from recursive_items(value, pattern=pattern)
            elif type(value) is list:
                for l in value:
                    yield from recursive_items(l, pattern=pattern)
    # yield (key, value)


def set_paths(test_name):
    """function setting local and thredds paths"""

    test_dir = 'Test' + test_name
    local_test_dir = os.path.join(local_root, 'ncdata_testNCML', test_dir)
    # hypothese : directory with nc data are in local_test_dir/test_name

    # set thredds paths
    thredds_test_dir = os.path.join(thredds_root, test_dir)
    if os.path.exists(thredds_test_dir):
        shutil.rmtree(thredds_test_dir)

    # delete local .ncmls
    for f in glob.glob(os.path.join(local_test_dir, '*.ncml')):
        os.remove(f)

    return local_test_dir, thredds_test_dir

class TestSchema:
    def test_valid_schema(self):
        validator = Validator("../schema/ncml-2.2.xsd")
        ncmls = list(path.Path("../1-Datasets").rglob('*.ncml'))
        assert len(ncmls) > 0
        for f in ncmls:
            print(f)
            assert validator.validate(str(f))




class TestDataset:

    # def test_CMIP5(self):
    #     test_reg = dict(lon=[-80, -70], lat=[45, 50])
    #     datasets = sorted(list(path.Path('../1-Datasets/simulations/cmip5/atmos').rglob('*.ncml')))
    #
    #     thredds_test_dir = f'{thredds_root}/simulations/cmip5/atmos'
    #     thredds_path_server = f'{thredds_cat_root}/simulations/cmip5/atmos/catalog.html'
    #     thredds_test_dir = path.Path(thredds_test_dir)
    #
    #     for ii, dataset in enumerate(datasets):
    #         if thredds_test_dir.exists():
    #             shutil.rmtree(thredds_test_dir)
    #         thredds_test_dir.mkdir(parents=True, exist_ok=True)
    #         print('trying', dataset.name)
    #         # copy to thredds:
    #         shutil.copy(dataset, thredds_test_dir)
    #
    #         ncmls_all = [ncml for ncml in threddsclient.crawl(thredds_path_server, depth=0)]
    #         rem1 = []
    #         ncmls = []
    #         for n in ncmls_all:
    #             if dataset.name  in n.name:
    #                 ncmls.append(n)
    #
    #         if len(ncmls) != 1:
    #             raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')
    #         #print('loading NcML via opendap')
    #         dsNcML = subset.subset_bbox(
    #             xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=256, lon=32, lat=32)),
    #             lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
    #         )
    #
    #         ncml = xncml.Dataset(dataset)
    #
    # def test_CMIP5_single(self):
    #     test_reg = dict(lon=[-80, -70], lat=[45, 50])
    #     datasets = sorted(list(path.Path('../tmp/simulations/cmip5_singlerun').rglob('*.ncml')))
    #
    #     thredds_test_dir = f'{thredds_root}/simulations/cmip5_singlerun'
    #     thredds_path_server = f'{thredds_cat_root}/simulations/cmip5_singlerun/catalog.html'
    #     thredds_test_dir = path.Path(thredds_test_dir)
    #
    #     for ii, dataset in enumerate(datasets):
    #         if thredds_test_dir.exists():
    #             shutil.rmtree(thredds_test_dir)
    #         thredds_test_dir.mkdir(parents=True, exist_ok=True)
    #         print('trying', dataset.name)
    #         # copy to thredds:
    #         shutil.copy(dataset, thredds_test_dir)
    #         ncmls_all = [ncml for ncml in threddsclient.crawl(thredds_path_server, depth=0)]
    #         ncmls = []
    #         for n in ncmls_all:
    #             if dataset.name in n.name:
    #                 ncmls.append(n)
    #
    #         if len(ncmls) != 1:
    #             raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')
    #
    #         dsNcML = subset.subset_bbox(
    #             xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=256, lon=32, lat=32) , drop_variables='time_bnds'),
    #             lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
    #         )
    #
    #         compare_ncml_rawdata(dataset, dsNcML)
    #
    # # def test_BCCAQv2(self):
    #
    #     datasets = sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip5/pcic/bccaqv2').rglob('*.ncml')))
    #
    #     thredds_test_dir = f'{thredds_root}/simulations/bias_adjusted/cmip5/pcic/bccaqv2'
    #     thredds_path_server = f'{thredds_cat_root}/simulations/bias_adjusted/cmip5/pcic/bccaqv2/catalog.html'
    #     thredds_test_dir = path.Path(thredds_test_dir)
    #
    #     for ii, dataset in enumerate(datasets):
    #         if thredds_test_dir.exists():
    #             shutil.rmtree(thredds_test_dir)
    #         thredds_test_dir.mkdir(parents=True, exist_ok=True)
    #         print('trying', dataset.name)
    #         # copy to thredds:
    #         shutil.copy(dataset, thredds_test_dir)
    #         ncmls_all = [ncml for ncml in threddsclient.crawl(thredds_path_server, depth=0)]
    #         ncmls = []
    #         for n in ncmls_all:
    #             if dataset.name in n.name:
    #                 ncmls.append(n)
    #
    #         if len(ncmls) != 1:
    #             raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')
    #
    #         dsNcML = subset.subset_bbox(
    #             xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=365, lon=50, lat=56)),
    #             lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
    #         )
    #
    #         compare_ncml_rawdata(dataset, dsNcML)

    # def test_OuraScenGen(self):
    #
    #     datasets = sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip5/ouranos/cb-oura-1.0').rglob('*.ncml')))
    #
    #     thredds_test_dir = f'{thredds_root}/simulations/bias_adjusted/cmip5/ouranos/cb-oura-1.0'
    #     thredds_path_server = f'{thredds_cat_root}/simulations/bias_adjusted/cmip5/ouranos/cb-oura-1.0/catalog.html'
    #     thredds_test_dir = path.Path(thredds_test_dir)
    #
    #     for ii, dataset in enumerate(datasets):
    #         if thredds_test_dir.exists():
    #             shutil.rmtree(thredds_test_dir)
    #         thredds_test_dir.mkdir(parents=True, exist_ok=True)
    #         print('trying', dataset.name)
    #         # copy to thredds:
    #         shutil.copy(dataset, thredds_test_dir)
    #         ncmls_all = [ncml for ncml in threddsclient.crawl(thredds_path_server, depth=0)]
    #         ncmls = []
    #         for n in ncmls_all:
    #             if dataset.name  in n.name:
    #                 ncmls.append(n)
    #
    #         if len(ncmls) != 1:
    #             raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')
    #
    #         dsNcML = subset.subset_bbox(
    #             xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=365, lon=50, lat=56)),
    #             lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
    #         )
    #
    #         compare_ncml_rawdata(dataset,dsNcML)

# def test_BCCAQv2(self):
    #
    #     datasets = sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip5/pcic/bccaqv2').rglob('*.ncml')))
    #
    #     thredds_test_dir = f'{thredds_root}/simulations/bias_adjusted/cmip5/pcic/bccaqv2'
    #     thredds_path_server = f'{thredds_cat_root}/simulations/bias_adjusted/cmip5/pcic/bccaqv2/catalog.html'
    #     thredds_test_dir = path.Path(thredds_test_dir)
    #
    #     for ii, dataset in enumerate(datasets):
    #         if thredds_test_dir.exists():
    #             shutil.rmtree(thredds_test_dir)
    #         thredds_test_dir.mkdir(parents=True, exist_ok=True)
    #         print('trying', dataset.name)
    #         # copy to thredds:
    #         shutil.copy(dataset, thredds_test_dir)
    #         ncmls_all = [ncml for ncml in threddsclient.crawl(thredds_path_server, depth=0)]
    #         ncmls = []
    #         for n in ncmls_all:
    #             if dataset.name in n.name:
    #                 ncmls.append(n)
    #
    #         if len(ncmls) != 1:
    #             raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')
    #
    #         dsNcML = subset.subset_bbox(
    #             xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=365, lon=50, lat=56)),
    #             lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
    #         )
    #
    #         compare_ncml_rawdata(dataset, dsNcML)

    # def test_OuraScenGen(self):
    #
    #     datasets = sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip5/ouranos/cb-oura-1.0').rglob('*.ncml')))
    #
    #     thredds_test_dir = f'{thredds_root}/simulations/bias_adjusted/cmip5/ouranos/cb-oura-1.0'
    #     thredds_path_server = f'{thredds_cat_root}/simulations/bias_adjusted/cmip5/ouranos/cb-oura-1.0/catalog.html'
    #     thredds_test_dir = path.Path(thredds_test_dir)
    #
    #     for ii, dataset in enumerate(datasets):
    #         if thredds_test_dir.exists():
    #             shutil.rmtree(thredds_test_dir)
    #         thredds_test_dir.mkdir(parents=True, exist_ok=True)
    #         print('trying', dataset.name)
    #         # copy to thredds:
    #         shutil.copy(dataset, thredds_test_dir)
    #         ncmls_all = [ncml for ncml in threddsclient.crawl(thredds_path_server, depth=0)]
    #         ncmls = []
    #         for n in ncmls_all:
    #             if dataset.name  in n.name:
    #                 ncmls.append(n)
    #
    #         if len(ncmls) != 1:
    #             raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')
    #
    #         dsNcML = subset.subset_bbox(
    #             xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=365, lon=50, lat=56)),
    #             lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
    #         )
    #
    #         compare_ncml_rawdata(dataset,dsNcML)

    def test_NEXGDDP(self):

        datasets = sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip5/nasa/nex-gddp-1.0').rglob('*.ncml')))

        thredds_test_dir = f'{thredds_root}/simulations/bias_adjusted/cmip5/nasa/nex-gddp-1.0'
        thredds_path_server = f'{thredds_cat_root}/simulations/bias_adjusted/cmip5/nasa/nex-gddp-1.0/catalog.html'
        thredds_test_dir = path.Path(thredds_test_dir)

        for ii, dataset in enumerate(datasets):
            if thredds_test_dir.exists():
                shutil.rmtree(thredds_test_dir)
            thredds_test_dir.mkdir(parents=True, exist_ok=True)
            print('trying', dataset.name)
            # copy to thredds:
            shutil.copy(dataset, thredds_test_dir)
            ncmls_all = [ncml for ncml in threddsclient.crawl(thredds_path_server, depth=0)]
            ncmls = []
            for n in ncmls_all:
                if dataset.name  in n.name:
                    ncmls.append(n)

            if len(ncmls) != 1:
                raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')

            dsNcML = subset.subset_bbox(
                xr.open_dataset(ncmls[0].opendap_url(), chunks='auto'),
                lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
            )

            compare_ncml_rawdata(dataset,dsNcML)

def compare_ncml_rawdata(dataset, dsNcML):
    ncml = xncml.Dataset(dataset)
    l1 = list(recursive_items(ncml.ncroot, '@location'))[0]

    if 'nasa/nex_gddp' not in l1[1]:
        key1 = '@location'
    else:
        key1 = '@regExp'
    for l in list(recursive_items(ncml.ncroot, key1)):

        mod = dataset.name.split('day_')[-1].split('_historical+')[0]
        rcp = dataset.name.split('_historical+')[-1][0:5]
        assert mod in l[1]
        assert ('historical' in l[1] or rcp in l[1])



    files = {}
    for l in list(recursive_items(ncml.ncroot, key1)):
        local_path = str(l[1].replace('pavics-data', pavics_root))
        #print(local_path)
        if path.Path(local_path).is_dir() or path.Path(local_path).is_file() or key1 == '@regExp' :
            if path.Path(local_path).is_file():
                ds = subset.subset_bbox(xr.open_dataset(path.Path(local_path),
                                        chunks=dict(time=10, lon=50, lat=50)),
                                        lon_bnds=test_reg['lon'],
                                        lat_bnds=test_reg['lat'],
                                        start_date=str(dsNcML.time.dt.year.min().values),
                                        end_date=str(dsNcML.time.dt.year.max().values))
            else:
                if key1 == '@location':
                    test_files = list(sorted(path.Path(local_path).rglob('*.nc')))
                else:
                    local_path = str(l1[1].replace('pavics-data', pavics_root))
                    test_files = list(sorted(path.Path(local_path).rglob(l[1].replace(r'.*\.nc$','*.nc'))))
                remove =[]
                for t in test_files:
                    #print(t)
                    y = netCDF4.Dataset(t)
                    time_y = y.variables['time']
                    dtime = netCDF4.num2date(time_y[:], units=time_y.units, calendar=time_y.calendar)
                    if dtime.max().year > 2100:
                        print('removing ' , t)
                        remove.append(t)
                for t in remove:
                    test_files.remove(t)
                run = path.Path(local_path).parent.name

                ds = subset.subset_bbox(xr.open_mfdataset(test_files,
                                       combine='by_coords',
                                       data_vars='minimal',
                                       chunks=dict(time=10, lon=50, lat=50)),
                                       lon_bnds=test_reg['lon'],
                                       lat_bnds=test_reg['lat'],
                                       start_date=str(dsNcML.time.dt.year.min().values),
                                       end_date=str(dsNcML.time.dt.year.max().values))

            if 'time_vectors' in ds.data_vars:
                ds = ds.drop_vars(['time_vectors','ts'])
            if 'time_bnds' in ds.data_vars:
                ds = ds.drop_vars(['time_bnds'])

            test = dsNcML.sel(time=ds.time).squeeze()
            for coord in ds.coords:
                np.testing.assert_array_equal(ds[coord].values, test[coord].values)
            time1 = np.random.choice(ds.time, 10)
            with ProgressBar():
                for v in ds.data_vars:
                    print(v)
                    if 'time' in ds[v].dims:
                        test2 = test[v].sel(time=time1).load()
                        if v == 'pr' and dsNcML[v].units == 'kg m-2 s-1':
                            np.testing.assert_array_almost_equal(ds[v].sel(time=time1).values*3600*24, test2*3600*24, decimal=2)
                        else:
                            np.testing.assert_array_almost_equal(ds[v].sel(time=time1).values, test2, decimal=3)
                    else:
                        np.testing.assert_array_almost_equal(ds[v].values, test[v].values)

    print(dataset,'ok')
    movfile = path.Path(str(dataset).replace('tmp','1-Datasets'))
    if not movfile.parent.exists():
        movfile.parent.mkdir(parents=True)
    shutil.move(dataset,movfile)


def main():
    test = TestDataset.test_NEXGDDP
    test(test)

if 'main' in __name__:
    main()