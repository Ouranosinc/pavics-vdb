import json
import os
import pathlib as p
from collections import OrderedDict
import collections
import xncml
import calendar
import xarray as xr

home = os.environ['HOME']
pavics_root = f"{home}/pavics/datasets"


def main():
    overwrite_to_tmp = True
    dataset_configs = p.Path(f"{home}/github/github_pavics-vdb/dataset_json_configs").rglob('*day*CRCM5-CMIP6*_config.json')
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
                run = keylist['@location'][0].split(exp)[-1].split('_195')[0].replace('_', '')
                outpath = p.Path(str(dataset.parent).replace('dataset_json_configs', 'tmp')).joinpath(
                    f"{ncml_modify['filename_template'].format(freq=ncml_modify['frequency'], model=d, run=run)}.ncml")
            elif ncml_modify['ncml_type'] == 'climatedata.ca':
                agg, freq = d.split('_')
                outpath = p.Path(str(dataset.parent).replace('dataset_json_configs',
                                                             'tmp')).joinpath(
                    agg,f"{dataset.name.split('_config')[0]}_{freq}.ncml".replace("__","_").replace('_.', '.'))
            elif ncml_modify['ncml_type'] == 'climatedata.ca_CanDCS-U6':
                agg, freq = d.split('_')
                outpath = p.Path(str(dataset.parent).replace('dataset_json_configs',
                                                             'tmp')).joinpath(
                    f"{agg}_{dataset.name.split('_config')[0]}.ncml".replace("__","_").replace('_.', '.'))
            elif ncml_modify['ncml_type'] == 'climatedata.ca_CanDCS-U6_members':
                agg, freq = d.split('_')
                outpath = p.Path(str(dataset.parent).replace('dataset_json_configs',
                                                             'tmp')).joinpath(
                    f"{agg}_{dataset.name.split('_config')[0]}.ncml".replace("__","_").replace('_.', '.'))
            else:
                if "separate_model_directory" in ncml_modify.keys():
                    if ncml_modify["separate_model_directory"] == "True":
                        mod = d.split('_hist')[0]
                        outpath = p.Path(str(dataset.parent).replace('dataset_json_configs', 'tmp')).joinpath(mod,
                                                                                                              f"{d}.ncml")


                else:
                    outpath = p.Path(str(dataset.parent).replace('dataset_json_configs', 'tmp')).joinpath(
                        f"{d}.ncml")

            if overwrite_to_tmp:
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

    elif config['ncml_type'] == 'multirun':
        ncmls = {}
        for exp in config['experiments']:
            agg_dict = {"@dimName": "realization", "@type": "joinNew", "variableAgg": config['variables']}
            agg = ncml_add_aggregation(agg_dict)
            freq = config['frequency']
            realm = config['realm']

            location = config['location'].replace('pavics-data', pavics_root)
            # add runs
            agg['netcdf'] = []
            for run in [x for x in p.Path(location).iterdir() if x.is_dir()]:
                print(run)
                netcdf = ncml_netcdf_container(dict={'@coordValue': run.name.split('-rcp')[0]})
                netcdf['aggregation'] = ncml_add_aggregation({"@type": "union"})
                netcdf['aggregation']['netcdf'] = []
                for v in config['variables']:
                    netcdf2 = ncml_netcdf_container()
                    netcdf2['aggregation'] = ncml_add_aggregation(
                        {'@dimName': 'time', '@type': 'joinExisting', '@recheckEvery': '1 day'})

                    reg_replace = r'.*\.'
                    reg1 = config['regexp_template'].format(v=v)
                    infiles = run.rglob(reg1)

                    scan = {'@location': run.as_posix().replace(pavics_root, 'pavics-data'),
                            '@regExp': f"{reg1.replace('*.', reg_replace)}$", '@subdirs': 'True',
                            '@suffix': '.nc'}

                    netcdf2['aggregation']['scan'] = ncml_add_scan(scan)
                    netcdf2['aggregation']['remove'] = []
                    netcdf2['aggregation']['remove'].append({"@name": "time_bnds", "@type": "variable"})
                    # netcdf2['aggregation']['remove'] = ncml_remove_items(config['remove_coords'])
                    netcdf['aggregation']['netcdf'].append(netcdf2)

                    del netcdf2

                agg['netcdf'].append(netcdf)
            ncml1 = xncml.Dataset(ncml_template)
            ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
            #            ncml1.ncroot['netcdf']['remove'].append(ncml_remove_items(config['remove_dims']))
            ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(config['attribute'])
            ncml1.ncroot['netcdf']['aggregation'] = agg
            ncmls[config['filename_template'].format(exp=exp)] = ncml1
            del ncml1

        return ncmls

    elif config['ncml_type'] == 'CRCM5-CMIP6':

        first_var = {"1hr":"tas", "3hr":"clwvi", "day":"tas"}
        config
        freq = config["frequency"]
        infolder = p.Path(config['location'].replace('/pavics-data', pavics_root))
        ncmls = {}
        for driver in [l for l in infolder.glob('*') if l.is_dir() ]:
            for exp in [ l for l in driver.glob('*') if l.is_dir() and l.name in config["experiments"]]:
                for run in [ l for l in exp.glob('*') if l.is_dir()]:
                    for mod in [ l for l in run.glob('*') if l.is_dir()]:
                        ncfiles = sorted(list(mod.rglob(f'*{freq}*.nc')))
                        time_per = f"{ncfiles[0].stem.split('_')[-1].split('-')[0]}-{ncfiles[-1].stem.split('_')[-1].split('-')[-1]}"
                        outname = f"{'_'.join(ncfiles[0].stem.split('_')[1:-1])}_{time_per}"

                        vars = [first_var[freq]]
                        vars.extend([v for v in sorted(list(set([f.name.split('_')[0] for f  in ncfiles]))) if v not in vars])
                        del ncfiles
                        agg_dict = {"@type": "Union"}
                        agg = ncml_add_aggregation(agg_dict)
                        agg['netcdf'] = []
                        for vv in vars:

                            scanloc = os.path.commonpath(sorted(list(mod.rglob(f'{vv}_*{freq}*.nc'))))
                            netcdf2 = ncml_netcdf_container()
                            netcdf2['aggregation'] = ncml_add_aggregation(
                                {'@dimName': 'time', '@type': 'joinExisting', '@recheckEvery': '1 day'})
                            netcdf2['aggregation']['scan'] = []
                            scan = {'@location': scanloc.replace(pavics_root, '/pavics-data'),
                                    '@subdirs': 'true',
                                    '@suffix': '*.nc'}
                            netcdf2['aggregation']['scan'].append(ncml_add_scan(scan))

                            agg['netcdf'].append(netcdf2)
                            del netcdf2
                        ncml1 = xncml.Dataset(ncml_template)
                        ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                        attrs = config['attribute']
                        #attrs['source_institution'] = dict(value=sim.name, type="String")
                        ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(attrs)
                        ncml1.ncroot['netcdf']['aggregation'] = agg

                        ncmls[outname] = ncml1
                        del ncml1

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
                        netcdf2['aggregation']['remove'].append({"@name": "time_vectors", "@type": "variable"})

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

    elif config['ncml_type'] == "ouranos-ESPO-R5":

        ncmls = {}
        location = config['location'].replace('pavics-data', pavics_root)
        for sim in [x for x in p.Path(location).iterdir() if x.is_dir()]:
            for rcm in [x for x in sim.iterdir() if x.is_dir()]:
                for gcm in [x for x in rcm.iterdir() if x.is_dir()]:
                    for rcp in [x for x in gcm.iterdir() if x.is_dir() and 'rcp' in x.name]:
                        var_flag = [len(list(rcm.rglob(f"*{v}_*.nc"))) > 0 for v in config['variables']]
                        agg_dict = {"@type": "Union"}
                        agg = ncml_add_aggregation(agg_dict)
                        agg['netcdf'] = []
                        outname = None
                        if all(var_flag):

                            netcdf = ncml_netcdf_container()
                            # ensure all variables are present
                            for v in config['variables']:
                                if outname is None:
                                    outname = '_'.join(list(rcp.rglob(f"*{v}_*.nc"))[0].stem.split('_')[1:-1])
                                    allfiles = sorted(list(rcp.rglob(f"*{v}_*.nc")))
                                    years = f"{allfiles[0].stem.split('_')[-1].split('-')[0]}-{allfiles[-1].stem.split('_')[-1].split('-')[-1]}"
                                    outname = '_'.join([outname, years])

                                netcdf2 = ncml_netcdf_container()
                                netcdf2['aggregation'] = ncml_add_aggregation(
                                    {'@dimName': 'time', '@type': 'joinExisting', '@recheckEvery': '1 day'})
                                netcdf2['aggregation']['scan'] = []
                                scan = {'@location': rcp.as_posix().replace(pavics_root, 'pavics-data'),
                                        '@subdirs': 'true',
                                        '@suffix': f'{v}_*.nc'}
                                netcdf2['aggregation']['scan'].append(ncml_add_scan(scan))

                                agg['netcdf'].append(netcdf2)
                                del netcdf2

                            ncml1 = xncml.Dataset(ncml_template)
                            ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                            attrs = config['attribute']
                            attrs['source_institution'] = dict(value=sim.name, type="String")
                            ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(attrs)
                            ncml1.ncroot['netcdf']['aggregation'] = agg

                            ncmls[outname] = ncml1
                            del ncml1
        return ncmls

    elif config['ncml_type'] == "ouranos-ESPO-G6":

        ncmls = {}
        location = config['location'].replace('pavics-data', pavics_root)
        for sim in [x for x in p.Path(location).iterdir() if x.is_dir()]:
            for gcm in [x for x in sim.iterdir() if x.is_dir()]:
                for scen in [x for x in gcm.iterdir() if x.is_dir() and 'ssp' in x.name]:
                    var_flag = [len(list(gcm.rglob(f"*{v}_*.nc"))) > 0 for v in config['variables']]
                    agg_dict = {"@type": "Union"}
                    agg = ncml_add_aggregation(agg_dict)
                    agg['netcdf'] = []
                    outname = None
                    if all(var_flag):

                        netcdf = ncml_netcdf_container()
                        # ensure all variables are present
                        for v in config['variables']:
                            if outname is None:
                                outname = '_'.join(list(scen.rglob(f"*{v}_*.nc"))[0].stem.split('_')[1:-1])
                                allfiles = sorted(list(scen.rglob(f"*{v}_*.nc")))
                                years = f"{allfiles[0].stem.split('_')[-1].split('-')[0]}-{allfiles[-1].stem.split('_')[-1].split('-')[-1]}"
                                outname = '_'.join([outname, years])

                            netcdf2 = ncml_netcdf_container()
                            netcdf2['aggregation'] = ncml_add_aggregation(
                                {'@dimName': 'time', '@type': 'joinExisting', '@recheckEvery': '1 day'})
                            netcdf2['aggregation']['scan'] = []
                            scan = {'@location': scen.as_posix().replace(pavics_root, 'pavics-data'),
                                    '@subdirs': 'true',
                                    '@suffix': f'{v}_*.nc'}
                            netcdf2['aggregation']['scan'].append(ncml_add_scan(scan))

                            agg['netcdf'].append(netcdf2)
                            del netcdf2

                        ncml1 = xncml.Dataset(ncml_template)
                        ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                        attrs = config['attribute']
                        attrs['source_institution'] = dict(value=sim.name, type="String")
                        ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(attrs)
                        ncml1.ncroot['netcdf']['aggregation'] = agg

                        ncmls[outname] = ncml1
                        del ncml1
        return ncmls

    elif config['ncml_type'] == "climatedata.ca":

        ncmls = {}
        location = p.Path(config['location'].replace('pavics-data', pavics_root))
        for aggkey, agg1 in config['aggregation'].items():
            for freq1, frequency1 in config['frequency'].items():
                if freq1 == 'YS':
                    freq1 = [freq1]
                elif freq1 == 'MS':
                    freq1 = [f"{str(x).zfill(2)}{calendar.month_name[x]}" for x in range(1, 13)]
                elif freq1 == 'QS-DEC':
                    freq1 = ['winterDJF', 'springMAM', 'summerJJA', 'fallSON']
                else:
                    raise Exception(f"unexpected frequency : {freq1}")

                for freq in freq1:
                    # runs = sorted(glob.glob(path.join(inrep1, "*" + m + "_hist*r*i1p1*195*2*" + f + "*.nc")))
                    agg_dict = {"@type": "Union"}
                    agg = ncml_add_aggregation(agg_dict)
                    # add runs
                    agg['netcdf'] = []
                    for v in [x for x in location.iterdir() if x.is_dir()]:
                        print(v)
                        for exp in config['experiments']:
                            realm = config['realm']
                            rcp = exp.split('+')[-1]

                            runs = sorted(list(location.joinpath(v).rglob(
                                config['regexp_template'].format(agg=aggkey, rcp=exp, v=v.name, frequency=freq))))
                            if len(runs) > 0:
                                netcdf2 = ncml_netcdf_container()
                                netcdf2['aggregation'] = ncml_add_aggregation(
                                    {'@dimName': 'time', '@type': 'joinExisting', '@timeUnitsChange': "true",
                                     '@recheckEvery': '1 day'})
                                netcdf2['aggregation']['netcdf'] = []
                                for run in runs:

                                    netcdf3 = ncml_netcdf_container()
                                    # r1 = runs.name.split(exp)[-1].split('_')[1]
                                    netcdf3['@location'] = str(run).replace(pavics_root, 'pavics-data')
                                    var_names = []
                                    try:
                                        dtmp = xr.open_dataset(run, engine="h5netcdf")
                                    except:
                                        dtmp = xr.open_dataset(run)

                                    for vv in dtmp.data_vars:
                                        if exp not in vv and exp != 'allrcps' and exp != 'nrcan':
                                            d1 = OrderedDict()

                                            d1["@name"] = f"{exp}_{vv}"
                                            d1["@orgName"] = vv
                                            var_names.append(d1)
                                            del d1
                                    del dtmp

                                    if var_names:
                                        netcdf3['variable'] = var_names
                                    netcdf2['aggregation']['netcdf'].append(netcdf3)
                                    del netcdf3
                                agg['netcdf'].append(netcdf2)
                                del netcdf2

                    ncml1 = xncml.Dataset(ncml_template)
                    ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                    attrs = config['attribute']
                    ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(attrs)
                    ncml1.ncroot['netcdf']['aggregation'] = agg
                    ncmls[f"{agg1}_{freq.replace('YS', 'annual')}"] = ncml1
                    del ncml1
        return ncmls

    elif config['ncml_type'] == "climatedata.ca_CanDCS-U6":

        ncmls = {}
        location = p.Path(config['location'].replace('pavics-data', pavics_root))
        for aggkey, agg1 in config['aggregation'].items():
            for freq, frequency1 in config['frequency'].items():
                # runs = sorted(glob.glob(path.join(inrep1, "*" + m + "_hist*r*i1p1*195*2*" + f + "*.nc")))
                agg_dict = {"@type": "Union"}
                agg = ncml_add_aggregation(agg_dict)
                # add runs
                agg['netcdf'] = []
                for v in sorted([x for x in location.iterdir() if x.is_dir()]):
                    print(v)
                    for exp in config['experiments']:
                        realm = config['realm']
                        rcp = exp.split('+')[-1]

                        runs = sorted(list(location.joinpath(v, freq).rglob(
                            config['regexp_template'].format(agg=agg1, rcp=exp, v=v.name, frequency=freq))))
                        if agg1 == '':
                            runs = [r for r in runs if '30ymean' not in r.name]
                        else:
                            runs = [r for r in runs if '30ymean' in r.name]
                        if len(runs) > 0:
                            netcdf2 = ncml_netcdf_container()
                            netcdf2['aggregation'] = ncml_add_aggregation(
                                {'@dimName': 'time', '@type': 'joinExisting', '@timeUnitsChange': "true",
                                 '@recheckEvery': '1 day'})
                            netcdf2['aggregation']['netcdf'] = []
                            for run in runs:

                                netcdf3 = ncml_netcdf_container()
                                # r1 = runs.name.split(exp)[-1].split('_')[1]
                                netcdf3['@location'] = str(run).replace(pavics_root, 'pavics-data')
                                var_names = []
                                try:
                                    dtmp = xr.open_dataset(run, engine="h5netcdf")
                                except:
                                    dtmp = xr.open_dataset(run)

                                for vv in dtmp.data_vars:
                                    if exp not in vv and exp != 'allrcps' and exp != 'nrcan':
                                        d1 = OrderedDict()

                                        d1["@name"] = f"{exp}_{vv}"
                                        d1["@orgName"] = vv
                                        var_names.append(d1)
                                        del d1
                                del dtmp

                                if var_names:
                                    netcdf3['variable'] = var_names
                                netcdf2['aggregation']['netcdf'].append(netcdf3)
                                del netcdf3
                            agg['netcdf'].append(netcdf2)
                            del netcdf2

                ncml1 = xncml.Dataset(ncml_template)
                ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                attrs = config['attribute']
                ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(attrs)
                ncml1.ncroot['netcdf']['aggregation'] = agg
                ncmls[f"{frequency1}_{agg1}"] = ncml1
                del ncml1
        return ncmls

    elif config['ncml_type'] == "climatedata.ca_CanDCS-U6_members":

        ncmls = {}
        location = p.Path(config['location'].replace('pavics-data', pavics_root))
        for aggkey, agg1 in config['aggregation'].items():
            for freq, frequency1 in config['frequency'].items():
                # runs = sorted(glob.glob(path.join(inrep1, "*" + m + "_hist*r*i1p1*195*2*" + f + "*.nc")))
                vars = sorted([x for x in location.iterdir() if x.is_dir()])
                varlist = []
                for exp in config['experiments']:
                    varlist.extend([f"{exp}_{v.name}" for v in vars])
                if agg1 == "30ymean":
                    varlist.extend([f"{v}_delta_1971_2000" for v in varlist])
                    varlist.extend([f"{v}_delta_1991_2020" for v in varlist])
                agg_dict = {"@dimName": "realization", "@type": "joinNew", "variableAgg": varlist} #
                agg = ncml_add_aggregation(agg_dict)
                # add runs
                agg['netcdf'] = []
                models = {'ACCESS-CM2': 'r1i1p1f1',
                         'ACCESS-ESM1-5': 'r1i1p1f1',
                         'BCC-CSM2-MR': 'r1i1p1f1',
                         'CMCC-ESM2': 'r1i1p1f1',
                         'CNRM-CM6-1': 'r1i1p1f2',
                         'CNRM-ESM2-1': 'r1i1p1f2',
                         'CanESM5': 'r1i1p2f1',
                         'EC-Earth3': 'r4i1p1f1',
                         'EC-Earth3-Veg': 'r1i1p1f1',
                         'FGOALS-g3': 'r1i1p1f1',
                         'GFDL-ESM4': 'r1i1p1f1',
                         'HadGEM3-GC31-LL': 'r1i1p1f3',
                         'INM-CM4-8': 'r1i1p1f1',
                         'INM-CM5-0': 'r1i1p1f1',
                         'IPSL-CM6A-LR': 'r1i1p1f1',
                         'KACE-1-0-G': 'r2i1p1f1',
                         'KIOST-ESM': 'r1i1p1f1',
                         'MIROC-ES2L': 'r1i1p1f2',
                         'MIROC6': 'r1i1p1f1',
                         'MPI-ESM1-2-HR': 'r1i1p1f1',
                         'MPI-ESM1-2-LR': 'r1i1p1f1',
                         'MRI-ESM2-0': 'r1i1p1f1',
                         'NorESM2-LM': 'r1i1p1f1',
                         'NorESM2-MM': 'r1i1p1f1',
                         'TaiESM1': 'r1i1p1f1',
                         'UKESM1-0-LL': 'r1i1p1f2'}

                for mod, rr in models.items():

                    netcdf2 = ncml_netcdf_container({"@coordValue":f"{mod}:{rr}"})
                    netcdf2['aggregation'] = ncml_add_aggregation({"@type": "Union"})
                    netcdf2['aggregation']['netcdf'] = []
                    for exp in config['experiments']:
                        print(mod, exp, rr)
                        for v in vars:

                            runs = sorted(list(location.joinpath(v, freq).rglob(
                                config['regexp_template'].format(agg=agg1, rcp=exp, mod=mod, v=v.name, frequency=freq))))
                            if agg1 == '':
                                runs = sorted([r for r in runs if '30ymean' not in r.name and 'simulations' in r.as_posix()])
                            else:
                                runs = sorted([r for r in runs if '30ymean' in r.name and 'simulations_30yAvg' in r.as_posix()])
                            if len(runs) > 0:
                                runs = [r for r in runs if f'{exp}_r10i' not in r.name]
                                run = runs[0] # take only first run
                                #coord1 = f"{mod}-{run.name.split('historical+ssp')[-1].split('_')[1]}"
                                #print(mod, exp, coord1)
                                #netcdf2['@coordValue'] = coord1
                                netcdf3 = ncml_netcdf_container()
                                # r1 = runs.name.split(exp)[-1].split('_')[1]
                                netcdf3['@location'] = str(run).replace(pavics_root, 'pavics-data')
                                var_names = []
                                try:
                                    dtmp = xr.open_dataset(run, engine="h5netcdf")
                                except:
                                    dtmp = xr.open_dataset(run)

                                for vv in dtmp.data_vars:
                                    if exp not in vv and exp != 'allrcps' and exp != 'nrcan':
                                        d1 = OrderedDict()

                                        d1["@name"] = f"{exp}_{vv}"
                                        d1["@orgName"] = vv
                                        var_names.append(d1)
                                        del d1
                                del dtmp

                                if var_names:
                                    netcdf3['variable'] = var_names
                                netcdf2['aggregation']['netcdf'].append(netcdf3)
                                del netcdf3
                            else:
                                print (f"no runs found for {freq}, {agg1}, {exp}, {mod}, {v.name} ... continuing")
                    agg['netcdf'].append(netcdf2)
                    del netcdf2



                ncml1 = xncml.Dataset(ncml_template)
                ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                attrs = config['attribute']
                ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(attrs)
                ncml1.ncroot['netcdf']['aggregation'] = agg
                ncmls[f"{frequency1}_{agg1}"] = ncml1
                del ncml1
        return ncmls

    elif config['ncml_type'] == "pcic-CanDCS-U6":
        ncmls = {}
        location = config['location'].replace('pavics-data', pavics_root)
        for mod in [x for x in p.Path(location).iterdir() if x.is_dir()]:

            for scen in config['experiments']:
                scen_reg = scen.replace('+', r'\+')
                runs = [x.stem.split(f"{scen}_")[-1].split('_')[0] for x in list(mod.rglob(f"*{scen}*.nc"))]
                runs = sorted(list(set(runs)))
                for run in runs:
                    print(mod, scen, run)
                    var_flag = [len(list(mod.rglob(f"*{v}_*{scen}_*{run}*.nc"))) > 0 for v in config['variables']]
                    agg_dict = {"@type": "Union"}
                    agg = ncml_add_aggregation(agg_dict)
                    agg['netcdf'] = []
                    outname = None
                    if all(var_flag):

                        # ensure all variables are present
                        for v in config['variables']:
                            search_str = f"*{v}_*{scen}_*{run}*.nc"
                            if outname is None:
                                outname = '_'.join(list(mod.rglob(search_str))[0].stem.split('_')[1:-1])
                                allfiles = sorted(list(mod.rglob(search_str)))
                                if len(allfiles) > 38:
                                    raise ValueError("too many files found")
                                years = f"{allfiles[0].stem.split('_')[-1].split('-')[0]}-{allfiles[-1].stem.split('_')[-1].split('-')[-1]}"
                                outname = '_'.join([outname, years])

                            netcdf2 = ncml_netcdf_container()
                            netcdf2['aggregation'] = ncml_add_aggregation(
                                {'@dimName': 'time', '@type': 'joinExisting', '@recheckEvery': '1 day'})
                            netcdf2['aggregation']['scan'] = []

                            scan = {'@location': mod.as_posix().replace(pavics_root, 'pavics-data'),
                                    '@subdirs': 'true',
                                    '@regExp': f".*{v}_.*{scen_reg}.*{run}.*",
                                    '@suffix': '.nc'}
                            netcdf2['aggregation']['scan'].append(ncml_add_scan(scan))

                            agg['netcdf'].append(netcdf2)
                            del netcdf2

                        ncml1 = xncml.Dataset(ncml_template)
                        ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                        attrs = config['attribute']

                        ncml1.ncroot['netcdf']['attribute'] = ncml_add_attributes(attrs)
                        ncml1.ncroot['netcdf']['aggregation'] = agg

                        ncmls[outname] = ncml1
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
                # runs = sorted(glob.glob(path.join(inrep1, "*" + m + "_hist*r*i1p1*195*2*" + f + "*.nc")))

                for v in config['variables']:
                    runs = sorted(list(location.glob(f"{v}*_{mod}_*{exp}*.nc")))
                    runs = runs[0]
                    r1 = runs.name.split(exp)[-1].split('_')[1]
                    netcdf2 = ncml_netcdf_container()
                    netcdf2['@location'] = str(runs).replace(pavics_root, 'pavics-data')
                    agg['netcdf'].append(netcdf2)
                    del netcdf2

                ncml1 = xncml.Dataset(ncml_template)
                ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                attrs = config['attribute'].copy()
                if mod == "NorESM1-ME":
                    attrs["driving_institution"] = {"value": "Norwegian Climate Center", "type": "String"}
                    attrs["driving_institute_id"] = {"value": "NCC", "type": "String"}
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
                # runs = sorted(glob.glob(path.join(inrep1, "*" + m + "_hist*r*i1p1*195*2*" + f + "*.nc")))

                for v in config['variables']:
                    netcdf2 = ncml_netcdf_container()
                    netcdf2['aggregation'] = ncml_add_aggregation(
                        {'@dimName': 'time', '@type': 'joinExisting', '@recheckEvery': '1 day'})
                    netcdf2['aggregation']['scan'] = []
                    for r in rcps:
                        reg_replace = r'.*\.'
                        reg1 = mod.name.replace('tasmin', v).replace('historical', r).replace('1950.',
                                                                                              '*.')  # "pr.*\.nc$"
                        infiles = location.rglob(reg1)

                        scan = {'@location': location.as_posix().replace(pavics_root, 'pavics-data'),
                                '@regExp': f"{reg1.replace('*.', reg_replace)}$", '@subdirs': 'True',
                                '@suffix': '.nc'}
                        netcdf2['aggregation']['scan'].append(ncml_add_scan(scan))

                    agg['netcdf'].append(netcdf2)
                    del netcdf2

                ncml1 = xncml.Dataset(ncml_template)
                ncml1.ncroot['netcdf']['remove'] = ncml_remove_items(config['remove'])
                attrs = config['attribute']
                attrs['driving_experiment'] = dict(value=exp.replace('+', ','), type='String')
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
        if 'value' in dict[d].keys():
            d1['@value'] = dict[d]['value']
        elif 'orgName' in dict[d].keys():
            d1['@orgName'] = dict[d]['orgName']
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
        if key == pattern or value == pattern:
            yield (key, value)
            if type(value) in [collections.OrderedDict, dict]:
                yield from recursive_items(value, pattern=pattern)
            elif type(value) is list:
                for l in value:
                    yield from recursive_items(l, pattern=pattern)
        else:
            if type(value) in [collections.OrderedDict, dict]:
                yield from recursive_items(value, pattern=pattern)
            elif type(value) is list:
                for l in value:
                    yield from recursive_items(l, pattern=pattern)
    # yield (key, value)


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
