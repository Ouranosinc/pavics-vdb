import copy
import json
import os
import pathlib as p
from collections import OrderedDict

import xncml

home = os.environ['HOME']
pavics_vdb_path = f"{home}/src/pavics-vdb"
dataset_id = 'QC11d3_CCCma-CanESM2_rcp85'
# test = xncml.Dataset(
#     f"{home}/src/pavics-vdb/1-Datasets/simulations/climex/atmos/day_QC11d3_CCCma-CanESM2_historical.ncml")
pavics_root = f"{home}/boreas"


def main():
    dataset_configs = p.Path(f"{pavics_vdb_path}/dataset_json_configs").rglob(f'*{dataset_id}*.json')
    for dataset in dataset_configs:
        with open(dataset, 'r') as f:
            ncml_modify = json.load(f)
        ncml_modify
        ncml_template = f'{pavics_vdb_path}/tests/test_data/NcML_templates/NcML_template_emptyNetcdf.ncml'
        ncml = xncml.Dataset(ncml_template)
        # ncml.ncroot['netcdf']
        if 'remove' in ncml_modify:
            ncml.ncroot['netcdf']['remove'] = ncml_remove_items(ncml_modify['remove'])
        if 'atribute' in ncml_modify:
            ncml.ncroot['netcdf']['attribute'] = ncml_add_attributes(ncml_modify['attribute'])

        datasets = ncml_create_datasets(ncml=ncml, config=ncml_modify)
        for d in datasets.keys():
            outpath = p.Path(str(dataset.parent).replace('dataset_json_configs', '1-Datasets')).joinpath(
                dataset.name.replace('config.json', f'{d}.ncml'))
            datasets[d].to_ncml(outpath)


def ncml_create_datasets(ncml=None, config=None):
    if config['ncml_type'] == 'cmip5-multirun':
        ncmls = {}
        for exp in config['experiments']:
            agg_dict = {"@dimName": "realization", "@type": "joinNew", "variableAgg": config['variables']}
            agg = ncml_add_aggregation(agg_dict)
            freq = config['frequency']
            realm = config['realm']

            location = config['location'].replace('pavics-data', pavics_root)
            # add runs
            agg['netcdf'] = []

            path1 = p.Path(location).joinpath('historical', freq, realm)
            for run in [x for x in path1.iterdir() if x.is_dir()]:
                netcdf = ncml_netcdf_container(dict={'@coordValue': run.name})
                netcdf['aggregation'] = ncml_add_aggregation({"@type": "union"})
                netcdf['aggregation']['netcdf'] = []
                for v in config['variables']:

                    netcdf2 = ncml_netcdf_container()
                    netcdf2['aggregation'] = ncml_add_aggregation(
                        {'@dimName': 'time', '@type': 'joinExisting', '@recheckEvery': '1 day'})
                    netcdf2['aggregation']['scan'] = []
                    rcps = exp.split('+')
                    for rcp in rcps:
                        path2 = str(path1.joinpath(run,v)).replace('historical',rcp)
                        scan = {'@location': path2.replace(pavics_root, 'pavics-data'), '@subdirs': 'false', '@suffix':'.nc'}
                        netcdf2['aggregation']['scan'].append(ncml_add_scan(scan))
                        del path2
                    netcdf['aggregation']['netcdf'].append(netcdf2)
                    del netcdf2
                agg['netcdf'].append(netcdf)
            agg
            ncml1 = copy.copy(ncml)
            ncml1.ncroot['netcdf']['aggregation'] = agg
            ncmls[exp] = ncml1

        return ncmls


def ncml_add_scan(dict=None):
    d1 = OrderedDict()
    for d in dict:
        d1[d] = dict[d]
    return d1


def ncml_netcdf_container(dict=None):
    d1 = OrderedDict()
    d1['@xmlns'] = 'http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2'
    if dict:
        for d in dict:
            d1[d] = dict[d]

    return d1


def ncml_add_aggregation(dict=None):
    agg = OrderedDict()
    for k in dict.keys():
        if 'variableAgg' in k:
            agg['variableAgg'] = []
            for v in dict['variableAgg']:
                d1 = OrderedDict()
                d1['@name'] = v
                agg['variableAgg'].append(d1)
                del d1
        else:
            agg[k] = dict[k]
    return agg


def ncml_add_attributes(dict=None):
    attrs = []
    for d in dict.keys():
        d1 = OrderedDict()
        d1['@name'] = d
        d1['@type'] = dict[d]['type']
        d1['@value'] = dict[d]['value']
        attrs.append(d1)
        del d1
    return attrs


def ncml_remove_items(dict=None):
    remove = []
    for d in dict.keys():
        d1 = OrderedDict()
        d1['@name'] = d
        d1['@type'] = dict[d]
        remove.append(d1)
        del d1
    return remove



if __name__ == "__main__":
    main()


