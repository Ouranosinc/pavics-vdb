import datetime
import itertools as it
import multiprocessing as mp
import os
import shutil
import tempfile
import time
import urllib
from pathlib import Path
import pandas as pd
import xarray as xr
from dask.diagnostics import ProgressBar

jobs = dict(GEPS=dict(inpath=Path('/tmp/geps_forecast/grib2'),  # download dir for grib2 files
                      # conversion output grib2 to nc
                      outpath=Path('/tmp/geps_forecast/netcdf'),
                      # "Birdhouse" datapath for combined .nc files TODO adjust to /pvcs1/DATA/eccc/forecasts/geps
                      threddspath=Path('/pvcs1/DATA/eccc/forecasts/geps'),
                      variables=dict(TMP_TGL_2m=dict(t2m='tas'), APCP_SFC_0=dict(paramId_0='pr')),
                      filename_pattern='CMC_geps-raw_{vv}_latlon0p5x0p5_{date}{HH}_P{hhh}_allmbrs.grib2',
                      urlroot='http://dd.weather.gc.ca/ensemble/geps/grib2/raw/',
                      time_expected=96,
                      pattern=['latlon0p5x0p5_', '_P'],
                      )
            )


def main():
    for j in jobs:
        # subscription missing files? patch with this
        time1 = time.time()
        print('Converting grib2 files to netcdf')

        ## This calls will download raw grib2 files from the eccc datamart
        if j == 'GEPS':
            update_dates = download_ddmart(j,
                            jobs[j]['urlroot'],
                            jobs[j]['filename_pattern'],
                            list(jobs[j]['variables'].keys()),
                            jobs[j]['inpath']
                            )


        ## We convert individual grib2 files to netcdf using an mp.pool()
        ## This makes subsequent xarray mutlifile dataset construnction much faster

        infiles = sorted(list(jobs[j]['inpath'].rglob('*.grib2')))  # list of all files

        # use todays date in order to decide which files to retain - delete the rest
        today = datetime.datetime.now()  #
        keep_days = datetime.timedelta(days=21)  # TODO adjust length of forecast to keep ... ~2-3 weeks??
        keepfiles = []
        deletefiles = []
        for i in infiles:
            forecast_date = i.name.split(jobs[j]['pattern'][0])[-1].split(jobs[j]['pattern'][1])[0][:-2]
            if (today - datetime.datetime.strptime(forecast_date, '%Y%m%d')) < keep_days:
                keepfiles.append(i)
            else:
                deletefiles.append(i)

        for d in deletefiles:
            # delete the .nc if on disk as well
            if jobs[j]['outpath'].joinpath(d.name.replace('.grib2', '.nc')).exists():
                os.remove(jobs[j]['outpath'].joinpath(d.name.replace('.grib2', '.nc')).as_posix())
            # delete grib2 file
            print(d)
            os.remove(d)

        # only convert grib2 files that have not already been converted
        allfiles = [i for i in keepfiles if not jobs[j]['outpath'].joinpath(i.name.replace('.grib2', '.nc')).exists()]

        outdir = jobs[j]['outpath']
        outdir.mkdir(parents=True, exist_ok=True)

        # create job list and execute with worker pool
        combs = list(it.product(*[allfiles, [outdir]]))
        pool = mp.Pool(15)
        pool.map(convert, combs)
        pool.close()
        pool.join()
        pool.terminate()

        print('done coverting ', j, '. It took : ', time.time() - time1, 'seconds')

        ## For each forcast date - Combine individual netcdfs files (all time-steps and variables) into an single .nc

        v = list(jobs[j]['variables'].keys())[0]
        # get list of unique forecast dates
        forecast_dates = sorted(list(set([n.name.split(jobs[j]['pattern'][0])[-1].split(jobs[j]['pattern'][1])[0]
                                          for n in jobs[j]['inpath'].glob(f"*{v}*.grib2")])))
        for f in forecast_dates:
            ncfiles = {}
            outfile = None
            for v in jobs[j]['variables']:
                ncfiles[v] = sorted(list(jobs[j]['outpath'].glob(f"*{v}*{f}*.nc")))
                if outfile is None:
                    outfile = jobs[j]['threddspath'].joinpath(
                        f"{ncfiles[v][0].name.split(jobs[j]['pattern'][0])[0]}{jobs[j]['pattern'][0]}{f}_allP_allmbrs.nc")
                    outfile = Path(outfile.as_posix().replace(v, '').replace('__', '_'))
                # print(f, v, len(ncfiles[v]))

            # make sure at least 90% of time steps are downloaded before combining
            expected_time = jobs[j]['time_expected'] - round(
                0.1 * jobs[j]['time_expected'])  # allow ~10% missing time steps
            if all([len(ncfiles[v]) > expected_time for v in ncfiles]):
                print(f"{f} : combining variables and timesteps ...")
                if (not outfile.exists()) | (f in update_dates):
                    reformat_nc((ncfiles, outfile, jobs[j]['variables']))

        ## update symlink recent forecast
        symlink = jobs[j]['threddspath'].joinpath('GEPS_latest.nc')
        latest = sorted([ll for ll in jobs[j]['threddspath'].glob('*.nc') if symlink.name not in ll.name])[-1]
        latest_date = latest.name.split(jobs[j]['pattern'][0])[-1].split('_allP')[0]

        # create symlink #TODO doesn't work via sshfs
        symlink.unlink(missing_ok=True)  # Delete first
        os.symlink(latest,symlink)

        opendap_latest = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/dodsC/datasets/forecasts/eccc_geps/GEPS_latest.ncml"
        if validate_ncml(opendap_latest, latest_date):
            print('success')
        else:
            # TODO how to handle unsuccessful update?
            print('update no good')


def validate_ncml(url, start_date):
    # Validate that ncml opendap link is functional and @location matches the most recent forecast .nc
    try:
        # Validate ncml opendap link works
        ds = xr.open_dataset(url)
        assert ds.reftime.values == pd.to_datetime(start_date, format='%Y%m%d%H')
        assert ds.time.isel(time=0).values == pd.to_datetime(start_date, format='%Y%m%d%H')#TODO implement validation
        return True
    except:
        raise Exception("can't read ncml opendap link")

def download_ddmart(job, urlroot, file_pattern, variables, outpath):
    today = datetime.datetime.now()
    dates = [today - datetime.timedelta(days=n) for n in range(0, 3)]
    update_dates = []
    for date in [d.strftime('%Y%m%d') for d in dates]:
        for HH in ['00', '12']:
            if job == 'GEPS':
                tt = list(range(0, 192, 3))
                tt.extend(list(range(192, 384 + 6, 6)))
            else:
                raise ValueError(f'Unknown forecast type "{job}"')
            print("Checking for updated GEPS files : Forecast", date, HH)
            newfiles = 0
            for vv in variables:

                for hhh in tt:
                    filename = outpath.joinpath(file_pattern.format(vv=vv, date=date, HH=HH, hhh=str(hhh).zfill(3)))
                    url = f'{urlroot}{HH}/{str(hhh).zfill(3)}/'
                    if not filename.exists():

                        try:
                            urllib.request.urlretrieve(f"{url}{filename.name}", filename.as_posix())
                            newfiles += 1
                        except:

                            continue
            print(f"Done. Found {newfiles} new files")
            if newfiles > 0:
                update_dates.append(f"{date}{HH}")
    return update_dates


def reformat_nc(job):
    ncfiles, outfile, var_dict = job

    print(outfile.name)
    with ProgressBar():
        ds_all = []
        for v in ncfiles:

            dstmp = xr.open_mfdataset(sorted(ncfiles[v]), combine='nested',
                                      chunks='auto', concat_dim='valid_time')
            for drop_var in ['surface', 'heightAboveGround']:
                try:
                    dstmp = dstmp.drop_vars(drop_var)
                except:
                    continue

            dstmp = dstmp.rename(var_dict[v])
            dstmp = dstmp.rename({'time': 'reftime',
                                  'valid_time': 'time',
                                  'longitude': 'lon',
                                  'latitude': 'lat',
                                  'number': 'member',
                                  }
                                 )
            ds_all.append(dstmp)

        ds = xr.merge(ds_all)
        ds.attrs = ds_all[0].attrs
        ds = ds.drop_vars('step')
        first_step = ds.isel(time=0)['pr'].fillna(0)
        ds['pr'] = xr.concat([first_step, ds.pr.isel(time=slice(1, None))], dim='time')

        ds = ds.transpose('member', 'time', 'lat', 'lon', )

        if 'tas' in ds.data_vars:
            ds['tas'] -= 273.15
            ds['tas'].attrs['units'] = 'degC'
        if 'pr' in ds.data_vars:
            ds['pr'].attrs['units'] = 'm'

        if not outfile.parent.exists():
            outfile.parent.mkdir(parents=True)

        ## TODO Running for multiple forecast shows RAM increasing over time?? Write nc in separate process for now
        # write_nc((ds,outfile))
        proc = mp.Process(target=write_nc, args=[[ds, outfile]])
        proc.start()
        proc.join()
        proc.close()


def write_nc(inputs):
    ds, outfile = inputs
    encoding = {}
    for c in ds.coords:
        if ds[c].dtype == 'int64':
            encoding[c] = {"dtype": "single"}
    for c in ds.data_vars:
        if 'heightAboveGround' == c or 'surface' == c:
            ds = ds.drop_vars(c)
        elif ds[c].dtype == 'int64':
            encoding[c] = {"dtype": "single"}
    encoding["time"] = {"dtype": "double"}
    encoding["member"] = {"dtype": "single"}
    encoding["reftime"] = {"dtype": "double"}
    try:
        mode = 'w'
        # TODO load() might not be necessary once running on boreas, VM write is slow so this speeds up the code for tests
        ds.load().to_netcdf(outfile, mode=mode, encoding=encoding, format='NETCDF4')
        ds.close()
        del ds,

    except:
        print('error exporting', outfile.as_posix())
        if outfile.exists():
            os.remove(outfile)


def convert(fn):
    """Convert grib2 file to netCDF format.

    TODO: Add parameter to change output directory.
    """
    try:
        infile, outpath = fn
        for f in Path(infile.parent).glob(infile.name.replace('.grib2','*.idx')):
            f.unlink(missing_ok=True)
        ds = xr.open_dataset(infile, engine="cfgrib", backend_kwargs={'filter_by_keys': {'dataType': 'pf'}}, chunks='auto')
        if 'number' in ds.dims:  # occasional files without number dimension?  Breaks concatenation : skip if not present
            encoding = {var: dict(zlib=True) for var in ds.data_vars}
            encoding["time"] = {"dtype": "single"}
            tmpfile = tempfile.NamedTemporaryFile(suffix='.nc', delete=False)
            with ProgressBar():
                print('converting ', infile.name)
                ds.to_netcdf(tmpfile.name, format='NETCDF4', engine="netcdf4", encoding=encoding)
            shutil.move(tmpfile.name, outpath.joinpath(infile.name.replace(".grib2", ".nc")).as_posix())

    except:

        print(f'error converting {infile.name} : File may be corrupted')
        #infile.unlink(missing_ok=True)
        pass


if __name__ == '__main__':
    main()
