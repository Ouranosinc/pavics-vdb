from os import path
import glob
import xarray as xr

def create_netcdf_testfiles():

    mods = ['CanESM2']

    for m in mods:
        # client = Client(dashboard_address=8989)
        inrep = '/home/travis/boreas/ouranos/cb-oura-1.0'
        ncfiles = sorted(glob.glob(path.join(inrep, m, 'rcp45', '*', 'tasmin', '*.nc')))

        for nc in ncfiles[0:5]:

            ds = xr.open_dataset(nc, chunks=dict(time=365, lat=50, lon=56), decode_times=False,
                                drop_variables=['ts', 'time_vectors']).sel(lon=-75, lat=45, method='nearest')

            outfile = glob.os.path.join(
                '/test_NcMLs/test_data/ncdata_testNCML',
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
                ds1.time.values = time_vals
            else:
                time_vals = ds1.time.values


            ds1.to_netcdf(
                outfile1,
            )


create_netcdf_testfiles()