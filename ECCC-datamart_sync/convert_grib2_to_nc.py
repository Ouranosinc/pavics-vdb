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
from contextlib import contextmanager
from dask.diagnostics import ProgressBar

dir_path = os.path.dirname(os.path.realpath(__file__))

# Default values set in `convert_grib2_to_nc.conf`.
CONVERT_GRIB2_TO_NC_INPATH_DEFAULT = ""
CONVERT_GRIB2_TO_NC_OUTPATH_DEFAULT = ""
CONVERT_GRIB2_TO_NC_THREDDSPATH_DEFAULT = ""

exec(open(f"{dir_path}/convert_grib2_to_nc.conf").read())

# inpath, outpath and threddspath dupes with run_convert_grib2_to_nc script.
jobs = dict(GEPS=dict(inpath=Path(os.environ.get(
                                  'CONVERT_GRIB2_TO_NC_INPATH',
                                  default=CONVERT_GRIB2_TO_NC_INPATH_DEFAULT)),  # download dir for grib2 files
                      # conversion output grib2 to nc
                      outpath=Path(os.environ.get(
                                   'CONVERT_GRIB2_TO_NC_OUTPATH',
                                   default=CONVERT_GRIB2_TO_NC_OUTPATH_DEFAULT)),
                      # "Birdhouse" datapath for combined .nc files
                      threddspath=Path(os.environ.get(
                                       'CONVERT_GRIB2_TO_NC_THREDDSPATH',
                                       default=CONVERT_GRIB2_TO_NC_THREDDSPATH_DEFAULT)),
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

        if not jobs[j]['inpath'].exists():
            jobs[j]['inpath'].mkdir(parents=True)
        if not jobs[j]['outpath'].exists():
            jobs[j]['outpath'].mkdir(parents=True)

        ## This calls will download raw grib2 files from the eccc datamart
        if j == 'GEPS':
            update_dates = []
            # run ~3 times to pick any possible missed downloads
            for i in range(0, 3):
                print(f'Downloading grib2 files : attempt {i + 1} of 3')
                update_dates.extend(download_ddmart(j, jobs[j]['urlroot'],
                                                    jobs[j]['filename_pattern'],
                                                    list(jobs[j]['variables'].keys()),
                                                    jobs[j]['inpath']
                                                    )
                                    )
            # get unique after loop
            update_dates = list(set(update_dates))

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
            #print(d)
            os.remove(d)

        # only convert grib2 files that have not already been converted
        allfiles = [i for i in keepfiles if not jobs[j]['outpath'].joinpath(i.name.replace('.grib2', '.nc')).exists()]

        outdir = jobs[j]['outpath']
        outdir.mkdir(parents=True, exist_ok=True)

        print('coverting to netcdf ....')

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
                if (not outfile.exists()) | (f in update_dates):
                    print(f"{f} : combining variables and timesteps ...")
                    reformat_nc((ncfiles, outfile, jobs[j]['variables']))
                else:
                    print(f"{f} : no action needed for combining variables and timesteps")

        ## update symlink recent forecast
        symlink = jobs[j]['threddspath'].joinpath(f'{j}_latest.nc')
        latest = sorted([ll for ll in jobs[j]['threddspath'].glob('*.nc') if symlink.name not in ll.name])[-1]
        latest_date = latest.name.split(jobs[j]['pattern'][0])[-1].split('_allP')[0]

        # create symlink
        symlink.unlink(missing_ok=True)  # Delete first
        os.chdir(symlink.parent)
        os.symlink(latest.name, symlink.name)

        opendap_latest = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/dodsC/datasets/forecasts/eccc_geps/GEPS_latest.ncml"
        if validate_ncml(opendap_latest, latest_date):
            print('success')
        else:
            # TODO how to handle unsuccessful update?
            print('update no good')

        # clean up files on thredds server
        sorted_files = sorted([ll for ll in jobs[j]['threddspath'].glob('*.nc') if symlink.name not in ll.name])[:-1]
        keepfiles = []
        deletefiles = []
        for i in sorted_files:
            forecast_date = i.name.split(jobs[j]['pattern'][0])[-1].split('_')[0][:-2]

            if (today - datetime.datetime.strptime(forecast_date, '%Y%m%d')) < keep_days:
                keepfiles.append(i)
            else:
                deletefiles.append(i)
        for d in deletefiles:
            os.remove(d)

def validate_ncml(url, start_date):
    # Validate that ncml opendap link is functional and @location matches the most recent forecast .nc
    try:
        # Validate ncml opendap link works
        ds = xr.open_dataset(url)
        assert ds.reftime.values == pd.to_datetime(start_date, format='%Y%m%d%H')
        assert ds.time.isel(time=0).values == pd.to_datetime(start_date, format='%Y%m%d%H')
        return True
    except:
        raise Exception("can't read ncml opendap link")


def download_ddmart(job, urlroot, file_pattern, variables, outpath):
    today = datetime.datetime.now()
    dates = [today - datetime.timedelta(days=n) for n in range(0, 3)]
    update_dates = []
    for date in [d.strftime('%Y%m%d') for d in dates]:
        for HH in ['00', '12']:
            # skip today's "12" forecast if script runs earlier than 12:00
            if datetime.datetime.strptime(f"{date}{HH}", '%Y%m%d%H') < today:
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
                                # urllib.request.urlretrieve(f"{url}{filename.name}", filename.as_posix())
                                request = urllib.request.urlopen(f"{url}{filename.name}", timeout=5)
                                with open(filename.as_posix(), 'wb') as f:

                                    f.write(request.read())
                                newfiles += 1


                            except:
                                time.sleep(0.1)
                                continue
                print(f"Done. Found {newfiles} new files")
                if newfiles > 0:
                    update_dates.append(f"{date}{HH}")
    return update_dates


def reformat_nc(job):
    ncfiles, outfile, var_dict = job

    print(outfile.name)

    with progressbar_toogle():
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
            ds['tas'].attrs['cell_methods'] = "time: mean"
        if 'pr' in ds.data_vars:
            ds['pr'].attrs['units'] = 'mm'
            ds['pr'].attrs['long_name'] = "depth of water-equivalent precipitation"
            ds['pr'].attrs['cell_methods'] = "time: sum"

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
    encoding = {var: dict(zlib=True) for var in ds.data_vars}
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
    """
    try:
        infile, outpath = fn
        for f in Path(infile.parent).glob(infile.name.replace('.grib2', '*.idx')):
            f.unlink(missing_ok=True)
        ds = xr.open_dataset(infile, engine="cfgrib", backend_kwargs={'filter_by_keys': {'dataType': 'pf'}},
                             chunks='auto')
        if 'number' in ds.dims:  # occasional files without number dimension?  Breaks concatenation : skip if not present
            encoding = {var: dict(zlib=True) for var in ds.data_vars}
            encoding["time"] = {"dtype": "single"}
            tmpfile = tempfile.NamedTemporaryFile(suffix='.nc', delete=False)

            with progressbar_toogle():
                print('converting ', infile.name)
                ds.to_netcdf(tmpfile.name, format='NETCDF4', engine="netcdf4", encoding=encoding)

            shutil.move(tmpfile.name, outpath.joinpath(infile.name.replace(".grib2", ".nc")).as_posix())

    except:

        print(f'error converting {infile.name} : File may be corrupted')
        # infile.unlink(missing_ok=True)
        pass


@contextmanager
def progressbar_toogle():
    """Drop-in replacement for the ProgressBar context manager.

    ProgressBar is not used if environment variable
    'CONVERT_GRIB2_TO_NC_PROGRESSBAR' exists and is set to 'false'.

    Useful in automation because ProgressBar generate lots of noises in logs.
    """

    try:
        if os.environ.get('CONVERT_GRIB2_TO_NC_PROGRESSBAR', default="") == 'false':
            # Not using ProgressBar.
            yield
        else:
            # Else use ProgressBar.
            with ProgressBar():
                yield
    finally:
        pass


if __name__ == '__main__':
    main()
