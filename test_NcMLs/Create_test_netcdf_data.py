from os import path
import glob
import xarray as xr
import numpy as np
import os

def create_netcdf_testfiles():

    mods = ['CanESM2']

    for m in mods:
        # client = Client(dashboard_address=8989)
        inrep = os.path.join(os.environ['HOME'], 'boreas/ouranos/cb-oura-1.0')
        ncfiles = sorted(glob.glob(path.join(inrep, m, 'rcp45', '*', 'tasmin', '*.nc')))

        for nc in ncfiles[0:5]:

            ds = xr.open_dataset(nc, chunks=dict(time=365, lat=50, lon=56), decode_times=False,
                                drop_variables=['ts', 'time_vectors']).sel(lon=-75, lat=45, method='nearest')

            outfile = glob.os.path.join(
                './test_data/ncdata_testNCML',
                'TimeConstant', f'TimeConstant_{nc.split("_")[-1]}')
            if not glob.os.path.exists(path.dirname(outfile)):
                glob.os.makedirs(path.dirname(outfile))


            ds.to_netcdf(
                outfile,
            )
            del ds
            ds1 = xr.open_dataset(nc, chunks=dict(time=365, lat=50, lon=56), decode_times=False,
                                drop_variables=['ts', 'time_vectors']).sel(lon=-75, lat=45, method='nearest')

            outfile1 = outfile.replace('TimeConstant', 'TimeChanging')
            if not glob.os.path.exists(path.dirname(outfile1)):
                glob.os.makedirs(path.dirname(outfile1))

            if nc != ncfiles[0]:
                ds1.time.attrs['units'] = f"days since {outfile1.split('_')[-1].split('.')[0]}-01-01 00:00:00"
                ds1.time.values = np.arange(0,len(ds1.time))
            else:
                time_vals = ds1.time.values


            ds1.to_netcdf(
                outfile1,
            )

        # create varying attribute between files
        # attr1 is set differently for every files except the before last one for which it is not set
        # attr2 is set differently for every files except the 1st file for which it is not set

        # third file does not have the attribute
        clef = './test_data/ncdata_testNCML/TimeConstant/*.nc'
        l_f = sorted(glob.glob(clef))

        out_dir = './test_data/ncdata_testNCML/TestVaryingAttributes/VaryingAttributes'
        if not path.exists(out_dir):
            os.makedirs(out_dir)

        for i_f, ff in enumerate(l_f):
            ds = xr.open_dataset(ff)

            # create attribute or not depending on file
            if i_f != 0:
                ds.attrs['attr2'] = str(i_f)
            if i_f != len(l_f)-2:
                ds.attrs['attr1'] = str(i_f)
            # save output file
            outpath = os.path.join(out_dir, os.path.basename(ff))
            ds.to_netcdf(outpath, format='NETCDF4_CLASSIC')

        # create nc files to test recursive ncml aggregation
        #
        inrep = os.path.join(os.environ['HOME'], 'boreas/ouranos/cb-oura-1.0')

        for varname in 'tasmin tasmax'.split():
            ncfiles1 = sorted(glob.glob(path.join(inrep, m, 'rcp45', '*', varname, '*.nc')))
            for ncf in ncfiles[0:2]:

                ds = xr.open_dataset(ncf, chunks=dict(time=365, lat=50, lon=56), decode_times=False,
                                    drop_variables=['ts', 'time_vectors']).sel(lon=-75, lat=45, method='nearest')

                outfile = glob.os.path.join(
                    './test_data/ncdata_testNCML', 'recursive_aggregation',
                    varname, varname + '_' + os.path.basename(ncf).split('_')[-1])
                if not glob.os.path.exists(path.dirname(outfile)):
                    glob.os.makedirs(path.dirname(outfile))

                ds.to_netcdf(
                    outfile,
                )

create_netcdf_testfiles()