import copy
import json
import os
import pathlib as p
from collections import OrderedDict
import collections
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
            if ncml_modify['ncml_type'] == 'pcic-bccaqv2':
                keylist = get_key_values(datasets[d].ncroot, searchkeys=['@location'])
                exp = f"historical{d.split('_historical')[-1]}"
                run = keylist['@location'][0].split(exp)[-1].split('_195')[0].replace('_','')
                outpath = p.Path(str(dataset.parent).replace('dataset_json_configs', 'tmp')).joinpath(
                    f"{ncml_modify['filename_template'].format(freq=ncml_modify['frequency'], model=d, run=run)}.ncml")
            else:
                if "separate_model_directory" in ncml_modify.keys():
                    if ncml_modify["separate_model_directory"]=="True":
                        mod = d.split('_hist')[0]
                        outpath = p.Path(str(dataset.parent).replace('dataset_json_configs', 'tmp')).joinpath(mod,
                            f"{d}.ncml")
                else:
                    outpath = p.Path(str(dataset.parent).replace('dataset_json_configs', 'tmp')).joinpath(
                    f"{d}.ncml")


            if not p.Path(outpath.parent.as_posix().replace('tmp','1-Datasets')).joinpath(outpath.name).exists():
                if not outpath.parent.exists():
                    outpath.parent.mkdir(parents=True)
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
                            path2 = str(path1.joinpath(run, v)).replace('historical', rcp)
                            scan = {'@location': path2.replace(pavics_root, 'pavics-data'), '@subdirs': 'false',
                                    '@suffix': '.nc'}
                            netcdf2['aggregation']['scan'].append(ncml_add_scan(scan))
                            del path2
                        netcdf['aggregation']['netcdf'].append(netcdf2)
                        del netcdf2
                agg['netcdf'].append(netcdf)
            agg
            ncml1 = xncml.Dataset(ncml_template)
            ncml1.ncroot['netcdf']['aggregation'] = agg
            ncmls[exp] = ncml1

        return ncmls

    elif config['ncml_type'] == 'cmip5-1run-batch-model':
        ncmls = {}
        for exp in config['experiments']:
            agg_dict = {"@dimName": "realization", "@type": "joinNew", "variableAgg": config['variables']}

            freq = config['frequency']
            realm = config['realm']

            location = config['location'].replace('pavics-data', pavics_root)
            for center in [x for x in p.Path(location).iterdir() if x.is_dir()]:
                print(center)
                for model in [x for x in p.Path(center).iterdir() if x.is_dir()]:

                    print(model)


                    path1 = p.Path(model).joinpath('historical', freq, realm)
                    if path1.exists():
                        run_flag = False
                        any_run = False
                        for run in [x for x in path1.iterdir() if x.is_dir()]:
                            agg_dict = {"@type": "Union"}
                            agg = ncml_add_aggregation(agg_dict)
                            # add runs
                            agg['netcdf'] = []
                            var_flag = [p.Path(run).joinpath(v).exists() for v in config['variables']]
                            if all(var_flag):
                                run_flag = True
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
                                        path2 = str(path1.joinpath(run, v)).replace('historical', rcp)
                                        if p.Path(path2).exists():
                                            scan = {'@location': path2.replace(pavics_root, 'pavics-data'),
                                                    '@subdirs': 'false', '@suffix': '.nc'}
                                            netcdf2['aggregation']['scan'].append(ncml_add_scan(scan))
                                            del path2
                                        else:
                                            run_flag = False
                                    netcdf['aggregation']['netcdf'].append(netcdf2)
                                    agg['netcdf'].append(netcdf2)
                                    del netcdf2

                                if run_flag:
                                    agg

                                    ncml1 = xncml.Dataset(ncml_template)
                                    ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                                    ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(config['attribute'])
                                    ncml1.ncroot['netcdf']['aggregation'] = agg
                                    ncmls[f'{model.name}_{exp}_{run.name}'] = ncml1
                                    del ncml1

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
                                run_flag = True
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
                                        path2 = str(path1.joinpath(run, v)).replace('historical', rcp)
                                        if p.Path(path2).exists():
                                            scan = {'@location': path2.replace(pavics_root, 'pavics-data'),
                                                    '@subdirs': 'false', '@suffix': '.nc'}
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

    elif config['ncml_type'] == "ouranos-cb-oura-1.0":

        model_dict = {}

        model_dict['BNU-ESM'] = dict(
            driving_institution="College of Global Change and Earth System Science, Beijing Normal University",
            driving_institute_id="GCESS")

        model_dict['CanESM2'] = dict(
            driving_institution="Canadian Centre for Climate Modelling and Analysis",
            driving_institute_id="CCCMA")

        model_dict['CMCC-CMS'] = dict(
            driving_institution="Centro Euro-Mediterraneo per I Cambiamenti Climatici",
            driving_institute_id="CMCC")

        model_dict['ACCESS1-3'] = dict(
            driving_institution="Commonwealth Scientific and Industrial Research Organization (CSIRO) and Bureau of Meteorology (BOM), Australia",
            driving_institute_id="CSIRO-BOM")

        model_dict['INM-CM4'] = dict(
            driving_institution="Institute for Numerical Mathematics",
            driving_institute_id="INM")

        model_dict['IPSL-CM5A-LR'] = dict(
            driving_institution="Institut Pierre-Simon Laplace",
            driving_institute_id="IPSL")

        model_dict['IPSL-CM5B-LR'] = dict(
            driving_institution="Institut Pierre-Simon Laplace",
            driving_institute_id="IPSL")

        model_dict['HadGEM2-CC'] = dict(
            driving_institution="Met Office Hadley Centre",
            driving_institute_id="MOHC")

        model_dict['MPI-ESM-LR'] = dict(
            driving_institution="Max-Planck-Institut für Meteorologie (Max Planck Institute for Meteorology)",
            driving_institute_id="MPI-M")

        model_dict['NorESM1-M'] = dict(
            driving_institution="Norwegian Climate Centre",
            driving_institute_id="NCC")

        model_dict['GFDL-ESM2M'] = dict(
            driving_institution="NOAA Geophysical Fluid Dynamics Laboratory",
            driving_institute_id="NOAA GFDL")

        ncmls = {}
        location = config['location'].replace('pavics-data', pavics_root)
        for center in [x for x in p.Path(location).iterdir() if x.is_dir()]:
            for exp in config['experiments']:
                agg_dict = {"@type": "Union"}
                agg = ncml_add_aggregation(agg_dict)
                # add runs
                agg['netcdf'] = []
                freq = config['frequency']
                realm = config['realm']
                rcp = exp.split('+')[-1]
                var_flag = [center.joinpath(rcp, freq, v, ).exists() for v in config['variables']]
                if all(var_flag):

                    netcdf = ncml_netcdf_container()
                    # ensure all variables are present
                    for v in config['variables']:
                        path2 = str(center.joinpath(rcp, freq, v, ))
                        print(path2)
                        netcdf2 = ncml_netcdf_container()
                        netcdf2['aggregation'] = ncml_add_aggregation(
                            {'@dimName': 'time', '@type': 'joinExisting', '@recheckEvery': '1 day'})
                        netcdf2['aggregation']['scan'] = []
                        scan = {'@location': path2.replace(pavics_root, 'pavics-data'), '@subdirs': 'false',
                                '@suffix': '.nc'}
                        netcdf2['aggregation']['scan'].append(ncml_add_scan(scan))
                        netcdf2['aggregation']['remove'] = []
                        netcdf2['aggregation']['remove'].append({"@name": "ts", "@type": "variable"})
                        netcdf2['aggregation']['remove'].append({"@name":"time_vectors","@type":"variable"})

                        agg['netcdf'].append(netcdf2)
                        del netcdf2

                ncml1 = xncml.Dataset(ncml_template)
                ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                attrs = config['attribute']
                for aa in model_dict[center.name]:
                    attrs[aa] = dict(value=model_dict[center.name][aa], type="String")
                attrs['driving_model'] = dict(value=center.name, type="String")
                ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(attrs)
                ncml1.ncroot['netcdf']['aggregation'] = agg

                ncmls[f'day_{center.name}_{exp}_r1i1p1_na10kgrid_qm-moving-50bins-detrend_1950-2100'] = ncml1
                del ncml1
        return ncmls

    elif config['ncml_type'] == "pcic-bccaqv2":
        mods = sorted(
            list(set(['BNU-ESM', 'CCSM4', 'CESM1-CAM5', 'CNRM-CM5', 'CSIRO-Mk3-6-0', 'CanESM2', 'FGOALS-g2', 'GFDL-CM3',
                      'GFDL-ESM2G', 'GFDL-ESM2M', 'HadGEM2-AO',
                      'HadGEM2-ES', 'IPSL-CM5A-LR', 'IPSL-CM5A-MR', 'MIROC-ESM-CHEM', 'MIROC-ESM', 'MIROC5',
                      'MPI-ESM-LR',
                      'MPI-ESM-MR', 'MRI-CGCM3', 'NorESM1-M', 'NorESM1-ME', 'bcc-csm1-1-m', 'bcc-csm1-1'])))
        assert len(mods) == 24
        ncmls = {}
        location = p.Path(config['location'].replace('pavics-data', pavics_root))
        for mod in mods:
            for exp in config['experiments']:
                agg_dict = {"@type": "Union"}
                agg = ncml_add_aggregation(agg_dict)
                # add runs
                agg['netcdf'] = []
                freq = config['frequency']
                realm = config['realm']
                rcp = exp.split('+')[-1]
                #runs = sorted(glob.glob(path.join(inrep1, "*" + m + "_hist*r*i1p1*195*2*" + f + "*.nc")))

                for v in config['variables']:
                    runs = sorted(list(location.glob(f"{v}*_{mod}_*{exp}*.nc")))
                    runs = runs[0]
                    r1 = runs.name.split(exp)[-1].split('_')[1]
                    netcdf2 = ncml_netcdf_container()
                    netcdf2['@location'] = str(runs).replace(pavics_root,'pavics-data')
                    agg['netcdf'].append(netcdf2)
                    del netcdf2

                ncml1 = xncml.Dataset(ncml_template)
                ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                attrs = config['attribute']
                ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(attrs)
                ncml1.ncroot['netcdf']['aggregation'] = agg
                ncmls[f'{mod}_{exp}'] = ncml1
                del ncml1
        return ncmls

    elif config['ncml_type'] == "nasa-nex_gddp-1.0":
        ncmls = {}
        location = p.Path(config['location'].replace('pavics-data', pavics_root))
        mods = list(location.rglob('*tasmin*historical*1950*.nc'))
        for mod in mods:
            for exp in config['experiments']:
                agg_dict = {"@type": "Union"}
                agg = ncml_add_aggregation(agg_dict)
                # add runs
                agg['netcdf'] = []
                freq = config['frequency']
                realm = config['realm']
                rcps = exp.split('+')
                #runs = sorted(glob.glob(path.join(inrep1, "*" + m + "_hist*r*i1p1*195*2*" + f + "*.nc")))

                for v in config['variables']:
                    netcdf2 = ncml_netcdf_container()
                    netcdf2['aggregation'] = ncml_add_aggregation(
                        {'@dimName': 'time', '@type': 'joinExisting', '@recheckEvery': '1 day'})
                    netcdf2['aggregation']['scan'] = []
                    for r in rcps:
                        reg_replace = r'.*\.'
                        reg1 = mod.name.replace('tasmin',v).replace('historical', r).replace('1950.','*.') #"pr.*\.nc$"
                        infiles = location.rglob(reg1)


                        scan = {'@location': location.as_posix().replace(pavics_root, 'pavics-data'), '@regExp': f"{reg1.replace('*.',reg_replace)}$", '@subdirs': 'True',
                                '@suffix': '.nc'}
                        netcdf2['aggregation']['scan'].append(ncml_add_scan(scan))


                    agg['netcdf'].append(netcdf2)
                    del netcdf2

                ncml1 = xncml.Dataset(ncml_template)
                ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                attrs = config['attribute']
                ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(attrs)
                ncml1.ncroot['netcdf']['aggregation'] = agg
                ncmls[f"day_{mod.name.split('i1p1_')[-1].split('_1950')[0]}_{exp}_nex-gddp"] = ncml1
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


if __name__ == "__main__":
    main()