import copy
import json
import os
import pathlib as p
from collections import OrderedDict

import xncml

home = os.environ['HOME']
pavics_root = f"{home}/boreas/boreas"


def main():
    dataset_configs = p.Path(f"{home}/github/github_pavics-vdb/dataset_json_configs").rglob('*.json')
    for dataset in dataset_configs:
        with open(dataset, 'r') as f:
            ncml_modify = json.load(f)
        ncml_modify
        ncml_template = f'{home}/github/github_pavics-vdb/tests/test_data/NcML_templates/NcML_template_emptyNetcdf.ncml'

        datasets = ncml_create_datasets(ncml_template=ncml_template, config=ncml_modify)
        for d in datasets.keys():
            outpath = p.Path(str(dataset.parent).replace('dataset_json_configs', '1-Datasets')).joinpath(
                f"{ncml_modify['frequency']}_{d}.ncml")
            datasets[d].to_ncml(outpath)


def ncml_create_datasets(ncml_template=None, config=None):
    if config['ncml_type'] == 'cmip5-multirun-single-model':
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
                    if p.Path(run).joinpath(v).exists():
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

    elif config['ncml_type'] == 'cmip5-multirun-batch-model':
        ncmls = {}
        for exp in config['experiments']:
            agg_dict = {"@dimName": "realization", "@type": "joinNew", "variableAgg": config['variables']}

            freq = config['frequency']
            realm = config['realm']

            location = config['location'].replace('pavics-data', pavics_root)
            for center in [x for x in p.Path(location).iterdir() if x.is_dir()]:
                print(center)
                for model in [x for x in p.Path(center).iterdir() if x.is_dir()]:
                    agg = ncml_add_aggregation(agg_dict)
                    print(model)
                    # add runs
                    agg['netcdf'] = []

                    path1 = p.Path(model).joinpath('historical', freq, realm)
                    if path1.exists():
                        run_flag = False
                        any_run = False
                        for run in [x for x in path1.iterdir() if x.is_dir()]:


                            var_flag = [p.Path(run).joinpath(v).exists() for v in config['variables']]
                            if all(var_flag):
                                run_flag=True
                                netcdf = ncml_netcdf_container(dict={'@coordValue': run.name})
                                netcdf['aggregation'] = ncml_add_aggregation({"@type": "union"})
                                netcdf['aggregation']['netcdf'] = []
                                # ensure all variables are present
                                for v in config['variables']:

                                    print(str(p.Path(run).joinpath(v)))
                                    netcdf2 = ncml_netcdf_container()
                                    netcdf2['aggregation'] = ncml_add_aggregation(
                                        {'@dimName': 'time', '@type': 'joinExisting', '@recheckEvery': '1 day'})
                                    netcdf2['aggregation']['scan'] = []
                                    rcps = exp.split('+')
                                    for rcp in rcps:
                                        path2 = str(path1.joinpath(run,v)).replace('historical',rcp)
                                        if p.Path(path2).exists():
                                            scan = {'@location': path2.replace(pavics_root, 'pavics-data'), '@subdirs': 'false', '@suffix':'.nc'}
                                            netcdf2['aggregation']['scan'].append(ncml_add_scan(scan))
                                            del path2
                                        else:
                                            run_flag = False
                                    netcdf['aggregation']['netcdf'].append(netcdf2)
                                    del netcdf2
                                if run_flag:
                                    any_run = True
                                    agg['netcdf'].append(netcdf)
                        # if at least one run has all variables add to list
                        if any_run:
                            ncml1 = xncml.Dataset(ncml_template)
                            ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                            ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(config['attribute'])
                            ncml1.ncroot['netcdf']['aggregation'] = agg
                            ncmls[f'{model.name}_{exp}'] = ncml1
                            del ncml1

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


