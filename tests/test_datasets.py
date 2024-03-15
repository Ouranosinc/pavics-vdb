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
import warnings
import random
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
    pavics_root = os.path.join(home, 'pavics/datasets')
    thredds_root = os.path.join(home, 'pavics/datasets/testdata/test_ncmls/')
    # correponding url to `thredds_root`
    thredds_cat_root = 'https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/birdhouse/testdata/test_ncmls'
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
            if type(value) in [ collections.OrderedDict, dict]:
                yield from recursive_items(value, pattern=pattern)
            elif type(value) is list:
                for l in value:
                    yield from recursive_items(l, pattern=pattern)
        else:
            if type(value) in [ collections.OrderedDict, dict]:
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

    def test_CMIP5(self):
        test_reg = dict(lon=[-80, -70], lat=[45, 50])
        datasets = sorted(list(path.Path('../1-Datasets/simulations/cmip5/atmos').rglob('*.ncml')))

        thredds_test_dir = f'{thredds_root}/simulations/cmip5/atmos'
        thredds_path_server = f'{thredds_cat_root}/simulations/cmip5/atmos/catalog.html'
        thredds_test_dir = path.Path(thredds_test_dir)

        for ii, dataset in enumerate(datasets):
            if thredds_test_dir.exists():
                shutil.rmtree(thredds_test_dir)
            thredds_test_dir.mkdir(parents=True, exist_ok=True)
            print('trying', dataset.name)
            # copy to thredds:
            shutil.copy(dataset, thredds_test_dir)

            ncmls_all = [ncml for ncml in threddsclient.crawl(thredds_path_server, depth=0)]
            rem1 = []
            ncmls = []
            for n in ncmls_all:
                if dataset.name  in n.name:
                    ncmls.append(n)

            if len(ncmls) != 1:
                raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')
            #print('loading NcML via opendap')
            dsNcML = subset.subset_bbox(
                xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=256, lon=32, lat=32)), decode_timedelta=False,
                lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
            )

            ncml = xncml.Dataset(dataset)

    def test_ClimateData(self, compare_raw=False):

        datasets = sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip5/climatedata_ca').rglob('*.ncml')))
        datasets.extend(sorted(list(path.Path('../tmp/gridded_obs/climatedata_ca').rglob('*.ncml'))))
        datasets.extend(sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip6/climatedata_ca').rglob('*.ncml'))))
        thredds_test_dir = f'{thredds_root}/simulations/climatedata_ca'
        thredds_path_server = f'{thredds_cat_root}/simulations/climatedata_ca/catalog.html'
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
                if dataset.name in n.name:
                    ncmls.append(n)

            if len(ncmls) != 1:
                raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')

            dsNcML = xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=50, lon=60, lat=60),decode_timedelta=False)
            sample_locations = None
            if 'realization' in dsNcML.dims:
                dsNcML = xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(realization=1, time=50, lon=60, lat=60),
                                         decode_timedelta=False)
                sample_locations = 0.15 # sample only 15% of all '@location' netcdfs
            #dsNcML = subset.subset_bbox(dsNcML, lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat'])
            dsNcML = dsNcML.sel(lon=slice(test_reg['lon'][0], test_reg['lon'][1]), lat=slice(test_reg['lat'][0], test_reg['lat'][1]))
            compare_ncml_rawdata(dataset, dsNcML, compare_raw, sample_location=sample_locations)

    def test_BCCAQv2(self, compare_raw=False):

            datasets = sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip5/pcic/bccaqv2').rglob('*.ncml')))

            thredds_test_dir = f'{thredds_root}/simulations/bias_adjusted/cmip5/pcic/bccaqv2'
            thredds_path_server = f'{thredds_cat_root}/simulations/bias_adjusted/cmip5/pcic/bccaqv2/catalog.html'
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
                    if dataset.name in n.name:
                        ncmls.append(n)

                if len(ncmls) != 1:
                    raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')

                dsNcML = subset.subset_bbox(
                    xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=365, lon=50, lat=56), decode_timedelta=False),
                    lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
                )

                compare_ncml_rawdata(dataset, dsNcML, compare_raw)

    def test_OuraScenGen(self, compare_raw=False):

        datasets = sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip5/ouranos/cb-oura-1.0').rglob('*.ncml')))

        thredds_test_dir = f'{thredds_root}/simulations/bias_adjusted/cmip5/ouranos/cb-oura-1.0'
        thredds_path_server = f'{thredds_cat_root}/simulations/bias_adjusted/cmip5/ouranos/cb-oura-1.0/catalog.html'
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
                xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=365, lon=50, lat=56), decode_timedelta=False),
                lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
            )

            compare_ncml_rawdata(dataset,dsNcML, compare_raw)

    def test_NEXGDDP(self, compare_raw=False):

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
                xr.open_dataset(ncmls[0].opendap_url(), chunks='auto', decode_timedelta=False),
                lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
            )

            compare_ncml_rawdata(dataset,dsNcML, compare_raw)

    def test_CRCM5_CMIP6(self, compare_raw=False):
        datasets = sorted(list(path.Path('../tmp/simulations/CMIP6/CORDEX/NAM-12').rglob('*.ncml')))

        thredds_test_dir = f'{thredds_root}/simulations/CMIP6/CORDEX/NAM-12'
        thredds_path_server = f'{thredds_cat_root}/simulations/CMIP6/CORDEX/NAM-12/catalog.html'
        thredds_test_dir = path.Path(thredds_test_dir)

        for ii, dataset in enumerate(datasets):
            if thredds_test_dir.exists():
                shutil.rmtree(thredds_test_dir)
            thredds_test_dir.mkdir(parents=True, exist_ok=True)
            print('trying', dataset.name)
            # copy to thredds:
            shutil.copy(dataset, thredds_test_dir)
            ncmls_all = [ncml for ncml in threddsclient.crawl(thredds_path_server, depth=5)]
            ncmls = []
            for n in ncmls_all:
                if dataset.name in n.name:
                    ncmls.append(n)

            if len(ncmls) != 1:
                raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')

            dsNcML = subset.subset_bbox(
                xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=250), decode_timedelta=False),
                lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
            )

            compare_ncml_rawdata(dataset, dsNcML, compare_raw, sample_time=False, files_perrun=5)

    def test_CLIMEX(self, compare_raw=False):

        datasets = sorted(list(path.Path('../tmp/simulations/climex').rglob('*.ncml')))

        thredds_test_dir = f'{thredds_root}/simulations/climex'
        thredds_path_server = f'{thredds_cat_root}/simulations/climex/catalog.html'
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
                if dataset.name in n.name:
                    ncmls.append(n)

            if len(ncmls) != 1:
                raise Exception(f'Expected a single ncml dataset : found {len(ncmls)}')

            dsNcML = subset.subset_bbox(
                xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=30, realization=1), decode_timedelta=False),
                lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
            )

            compare_ncml_rawdata(dataset, dsNcML, compare_raw, check_times=False, files_perrun=3)

    def test_ESPO_R(self, compare_raw=False):

        datasets = sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip5/ouranos/ESPO-R/ESPO-R5v1.0.0').rglob('*.ncml')))

        thredds_test_dir = f'{thredds_root}/simulations/bias_adjusted/cmip5/ouranos/ESPO-R/ESPO-R5v1.0.0'
        thredds_path_server = f'{thredds_cat_root}/simulations/bias_adjusted/cmip5/ouranos/ESPO-R/ESPO-R5v1.0.0/catalog.html'
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
                xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=1460, lon=50, lat=50), decode_timedelta=False),
                lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
            )

            compare_ncml_rawdata(dataset,dsNcML, compare_raw)

    def test_ESPO_G(self, compare_raw=False):

        datasets = sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip6/ouranos/ESPO-G/').rglob('*.ncml')))

        thredds_test_dir = f'{thredds_root}/simulations/bias_adjusted/cmip6/ouranos/ESPO-G/'
        thredds_path_server = f'{thredds_cat_root}/simulations/bias_adjusted/cmip6/ouranos/ESPO-G/catalog.html'
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
            if "ESPO-G6-R2" in dataset.name:
                chunks=dict(time=1460/4, rlon=150, rlat=150)
            else:
                chunks = dict(time=1460, lon=50, lat=50)
            dsNcML = xr.open_dataset(ncmls[0].opendap_url(), chunks=chunks, decode_timedelta=False)
            for ll in ['lat','lon']:
                with ProgressBar():
                    dsNcML[ll] = dsNcML[ll].load()
            dsNcML = subset.subset_bbox(dsNcML,
                lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
            )

            compare_ncml_rawdata(dataset,dsNcML, compare_raw)

    def test_CanDCS_U6(self, compare_raw=False):

        datasets = sorted(list(path.Path('../tmp/simulations/bias_adjusted/cmip6/pcic/CanDCS-U6').rglob('*.ncml')))

        thredds_test_dir = f'{thredds_root}/simulations/bias_adjusted/cmip6/pcic/CanDCS-U6'
        thredds_path_server = f'{thredds_cat_root}/simulations/bias_adjusted/cmip6/pcic/CanDCS-U6/catalog.html'
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
                xr.open_dataset(ncmls[0].opendap_url(), chunks=dict(time=1460, lon=50, lat=50), decode_timedelta=False),
                lon_bnds=test_reg['lon'], lat_bnds=test_reg['lat']
            )

            compare_ncml_rawdata(dataset,dsNcML, compare_raw)

def compare_ncml_rawdata(dataset, dsNcML, compare_vals, sample_time=True, files_perrun=None, sample_location=None):
    ncml = xncml.Dataset(dataset)
    l1 = list(recursive_items(ncml.ncroot, '@location'))[0]

    if 'bccaqv2' in l1[1] or 'cccs_portal' in l1[1]:
        key1 = '@location'
    else:
        key1 = 'scan'


    if 'climex' not in l1[1] and 'cccs_portal' not in l1[1] and 'ESPO' not in l1[1] and "CanDCS-U6" not in l1[1] and "CORDEX" not in l1[1] :
        for l in list(recursive_items(ncml.ncroot, key1)):
            mod = dataset.name.split('day_')[-1].split('_historical+')[0]
            rcp = dataset.name.split('_historical+')[-1][0:5]
            assert mod in l[1]['@location']
            assert ('historical' in l[1]['@location'] or rcp in l[1]['@location'])

    files = {}

    locations = list(recursive_items(ncml.ncroot, key1))
    if sample_location:
        ind1 = np.random.choice(range(0,len(locations)), round(sample_location * len(locations) ), replace=False)
        loc1 = []
        for ii in ind1:
            loc1.append(locations[ii])
        locations = loc1
        del loc1
    for l in locations:
        if key1 == "scan":
            l = l[1]
        if isinstance(l,collections.OrderedDict) or isinstance(l, dict):
            local_path = str(l['@location'].replace('pavics-data', pavics_root))
        else:
            local_path = str(l[1].replace('pavics-data', pavics_root))
        #print(local_path)
        if path.Path(local_path).is_dir() or path.Path(local_path).is_file() or key1 == '@regExp' :
            if path.Path(local_path).is_file():
                ds = subset.subset_bbox(xr.open_dataset(path.Path(local_path), decode_timedelta=False,
                                        engine="h5netcdf",
                                        chunks=dict(time=30, lon=30, lat=30)),
                                        lon_bnds=test_reg['lon'],
                                        lat_bnds=test_reg['lat'],
                                        start_date=str(dsNcML.time.dt.year.min().values),
                                        end_date=str(dsNcML.time.dt.year.max().values))
            else:
                str1 = ''

                if '@suffix' in l.keys():
                    str1 = f"*{l['@suffix']}".replace('**','*')

                if '@regExp' in l.keys():
                    regexp = l['@regExp'].replace(r'\.nc$', '.nc').replace('.*', '*')
                    str1 = f"{regexp.replace(str1,'')}{str1}".replace('**','*').replace('\\','') # regexp can occasionally already have suffix replace double

                if '@subdirs' in l.keys():
                    # use rglob
                    if l['@subdirs'].lower() == 'true':
                        test_files = list(sorted(path.Path(local_path).rglob(str1)))
                    else:
                        test_files = list(sorted(path.Path(local_path).glob(str1)))
                else:
                    test_files = list(sorted(path.Path(local_path).glob(str1)))
                # //home/logan/pavics/datasets/disk2/ouranos/CORDEX/CMIP6/DD/NAM-12/OURANOS/CNRM-ESM2-1/historical/r1i1p1f2/CRCM5/v1-r1/1hr/hurs/v20240201/hurs_NAM-12_CNRM-ESM2-1_historical_r1i1p1f2_OURANOS_CRCM5_v1-r1_1hr_197701010000-197712312300.nc
                test_files
                # remove =[]
                # if check_times:
                #     for t in test_files:
                #         print(t)
                #         y = netCDF4.Dataset(t)
                #         time_y = y.variables['time']
                #         warnings.simplefilter('ignore')
                #         dtime = xr.DataArray(netCDF4.num2date(time_y[:], units=time_y.units, calendar=time_y.calendar))
                #         if dtime.dt.year.max() > 2100:
                #             print('removing ' , t)
                #             remove.append(t)
                #
                # for t in remove:
                #     test_files.remove(t)

                run = path.Path(local_path).parent.name
                datasets = []
                if files_perrun is None or len(test_files) <= files_perrun:
                    ds = subset.subset_bbox(xr.open_mfdataset(test_files,
                                           engine="h5netcdf",
                                           decode_timedelta=False,
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
                    datasets.append(ds)
                    #compare_values(dsNcML, ds, compare_vals)
                else:
                    for file1 in random.sample(test_files, files_perrun):
                        print(file1)
                        ds = subset.subset_bbox(xr.open_dataset(file1, chunks=dict(time=-1), engine='h5netcdf', decode_timedelta=False),
                                                lon_bnds=test_reg['lon'],
                                                lat_bnds=test_reg['lat'],
                                                )
                        datasets.append(ds)
            for ds in datasets:
                if 'climex' in l1:
                    compare_values(dsNcML.sel(realization=bytes(file1.parent.name.split('-rcp')[0],  'utf-8')), ds, compare_vals, sample_time=sample_time)

                else:
                    if 'cccs_portal' in l1[1]:
                        if 'realization' in dsNcML.dims:

                            dstest = dsNcML.sel(realization=[f"{str(r.values).split(':r')[0]}_" in path.Path(l[1]).name for r in
                                                             dsNcML.realization.astype(str)]).squeeze()
                        else:
                            dstest = dsNcML

                        if 'realization' in dstest.dims:
                            raise ValueError('test data has realization dimension')
                        rcp = [rcp for rcp in ['rcp26','rcp45','rcp85','ssp126','ssp245','ssp585'] if rcp in local_path]
                        if len(rcp)>1:
                            raise ValueError(f'expected single rcp value found {rcp}')
                        rcp = rcp[0]
                        print(local_path, rcp)
                        for v in ds.data_vars:
                            if v not in dsNcML.data_vars and f"{rcp}_{v}" in dsNcML.data_vars:
                                ds = ds.rename({v:f"{rcp}_{v}"})
                        del rcp

                        compare_values(dstest, ds, compare_vals, sample_time=sample_time)
                    else:
                        compare_values(dsNcML, ds, compare_vals, sample_time=sample_time)





    print(dataset,'ok')
    movfile = path.Path(str(dataset).replace('tmp','1-Datasets'))
    if not movfile.parent.exists():
        movfile.parent.mkdir(parents=True)
    shutil.move(dataset,movfile)

def compare_values(dsNcML, ds, compare_vals, sample_time=True):

    try:
        test = dsNcML.sel(time=ds.time).squeeze()
    except:

        time1 = xr.DataArray(list(ds.resample(time=xr.infer_freq(dsNcML.time)).groups.keys()), dims=['time'])
        ds = ds.assign_coords(time=time1)
        time2 = xr.DataArray(list(dsNcML.resample(time=xr.infer_freq(dsNcML.time)).groups.keys()), dims=['time'])
        dsNcML = dsNcML.assign_coords(time=time2)
        test = dsNcML.sel(time=ds.time).squeeze()
        del time1, time2

    for coord in ds.coords :
        if coord not in ['height', 'horizon', 'time_bnds' ] :
            np.testing.assert_array_equal(ds[coord].values, test[coord].values)
    if sample_time:
        time1 = np.random.choice(ds.time, 15)
    else:
        time1 = ds.time
    if compare_vals:
        with ProgressBar():
            for v in ds.data_vars:
                if v not in ['time_bnds','lat','lon'] and ds[v].dtype != 'S1':
                    print(v)
                    if 'time' in ds[v].dims:
                        test1 = ds[v].sel(time=time1).load()
                        test2 = test[v].sel(time=time1).load()
                        if v in ['pr','prsn'] and dsNcML[v].units == 'kg m-2 s-1':
                            np.testing.assert_array_almost_equal(test1 * 3600 * 24,

                                                                 test2 * 3600 * 24, decimal=2)
                        else:
                            np.testing.assert_array_almost_equal(test1, test2, decimal=3)
                    else:
                        np.testing.assert_array_almost_equal(ds[v].values, test[v].values)


def main():
    #test = TestDataset.test_OuraScenGen
    # test(self=test,compare_raw=False)
    #test = TestDataset.test_BCCAQv2
    #test(self=test, compare_raw=False)
    #test = TestDataset.test_NEXGDDP
    #test = TestDataset.test_CLIMEX
    #test = TestDataset.test_ClimateData
    #test = TestDataset.test_ESPO_R
    #test = TestDataset.test_ESPO_G
    #test = TestDataset.test_CanDCS_U6
    test = TestDataset.test_CRCM5_CMIP6
    test(self=test, compare_raw=True)

if 'main' in __name__:
    main()