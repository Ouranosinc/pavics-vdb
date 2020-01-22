import xncml
import xarray as xr
import os
import collections
import glob
from os.path import expanduser

def recursive_items(dictionary, pattern):
    for key, value in dictionary.items():
        if key == pattern or value==pattern:
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
    #yield (key, value)

def get_key_values(ncml, searchkeys=None):
    key_vals = {}
    for s in searchkeys:
        key_vals[s] = []

        for key, value in recursive_items(ncml, s):
            print(key, value)
            if key in searchkeys or value in searchkeys:
                key_vals[key].append(value)

    return key_vals

def get_scan_keys(keylist,location=None):
    for k in keylist:
        if k['@location'] == location:
            return k

def xarray_opennmcl(ncml, tdsroot=None, **xr_kwargs):
    # ncml is path to .ncml dataset
    # tdsroot is the local path to the thredds root directory - needed if using absolute path
    ncml_dict = xncml.Dataset(ncml).ncroot


    # get all aggregations in the .ncml
    keylist = get_key_values(ncml_dict, searchkeys=['@location','scan'])

    # filter results for those with 'scan' information
    scan_locs = [loc['@location'] for loc in keylist['scan']]

    ncfiles = []
    for a in keylist['@location']:
        if a in scan_locs:
            #get scan info keys
            scan = get_scan_keys(keylist['scan'],a)
            if '@suffix' not in scan.keys():
                scan['@suffix'] = None

            if scan['@location'][0] != '/': # location is relative path
                datapath = os.path.join(os.path.dirname(ncml),scan['@location'], f"{'*'}{scan['@suffix']}")
            else:
                if tdsroot:
                    datapath = os.path.join(tdsroot, scan['@location'],f"{'*'}{scan['@suffix']}")
                else:
                    #TODO default is os.path.abspath?
                    raise NotImplementedError('NcMLs with absolute path location require user input `tdsroot` parameter '
                                              'indicating the root data directory')
                    #datapath = os.path.abspath(scan['@location'])
            if '@subdirs' in scan.keys():

                if scan['@subdirs'] == 'false':

                    ncfiles.extend(sorted(glob.glob(datapath)))
                else:

                    ncfiles.extend(
                        sorted(glob.glob(datapath, recursive=True)))  # TODO crawl subdirs if @subdirs True

        else:
            if a[0] != '/':
                datapath = os.path.join(os.path.dirname(ncml), a)
                ncfiles.extend(sorted(glob.glob(datapath)))
            else:
                if tdsroot:
                    ncfiles.append(os.path.join(tdsroot,*os.path.split(a)[1:]))
                else:
                    #TODO default is os.path.abspath?
                    raise NotImplementedError('NcMLs with absolute path location require user input `tdsroot` parameter '
                                                          'indicating the root data directory')

    ds = xr.open_mfdataset(ncfiles, combine='by_coords', **xr_kwargs)
    return ds


home = expanduser("~")
ds = xarray_opennmcl(f'{home}/github/github_pavics-vdb/test_NcMLs/test_data/ncdata_testNCML/MFDataset/NcML_Union.ncml')

print(ds)