import xncml
import xarray as xr
import os
import collections
import glob
from os.path import expanduser

def recursive_items(dictionary):
    for key, value in dictionary.items():
        if type(value) is collections.OrderedDict:
            yield from recursive_items(value)
        elif type(value) is list:
            for l in value:
                yield from recursive_items(l)
        else:
            yield (key, value)

def get_key_values(ncml, searchkeys):
    key_vals = {}
    for s in searchkeys:
        key_vals[s] = []

    for key, value in recursive_items(ncml):
        #print(key, value)
        if key in searchkeys:
            key_vals[key].append(value)

    return key_vals

def xarray_opennmcl(ncml, tdsroot=None, **xr_kwargs):
    # ncml is path to .ncml dataset
    # tdsroot is the local path to the thredds root directory - needed if using absolute path
    ncml_dict = xncml.Dataset(ncml).ncroot
    locations = get_key_values(ncml_dict, searchkeys=['@location','@subdirs','@suffix'])
    ncfiles = []
    for i, l in enumerate(locations['@location']):

        if l[0] != '/': # location is relative path
            if locations['@subdirs'][i] == 'false':
                datapath = os.path.join(os.path.dirname(ncml),l,f"*{locations['@suffix'][i]}")
                ncfiles.extend(sorted(glob.glob(datapath)))
            else:
                #TODO crawl subdirs if @subdirs True
                NotImplementedError()
        else:
            #TODO implement absolute path
            l

    print(ncfiles)

    ds = xr.open_mfdataset(ncfiles, combine='by_coords', **xr_kwargs)
    return ds


home = expanduser("~")
ds = xarray_opennmcl(f'{home}/github/github_pavics-vdb/test_NcMLs/test_data/ncdata_testNCML/MFDataset/NcML_Union.ncml')
print(ds)