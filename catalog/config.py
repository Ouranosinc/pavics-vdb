import os

# THREDDS catalog root URL
TDS = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/datasets"

# Mapping of CV registered specs to TDS paths for public catalogs.
_tds_path = {"cmip5": "simulations/cmip5_multirun",
             "biasadjusted": "simulations/bias_adjusted",
             "climex": "simulations/climex",
             "reanalysis": "reanalyses",
             "gridobs": "gridded_obs",
             "stationobs": "station_obs",
             "forecast": "forecasts"}

# Mapping of CV registered specs to local file paths for private catalogs.
_disk_path = {"cordex": ["/tank/scenario/external_data/CORDEX/",
                         "/tank/scenario/external_data/CORDEX-AFR/",
                         "/tank/scenario/external_data/CORDEX-NA/"],
              "reanalysis": ["/tank/scenario/external_data/nasa/agcfsr/",
                             "/tank/scenario/external_data/nasa/agmerra/",
                             "/tank/scenario/external_data/nasa/merra/"],
              "gridobs": ["/tank/scenario/netcdf/nrcan/nrcan_canada_daily_v2/",
                          "/tank/scenario/netcdf/nrcan/nrcan_northamerica_monthly/",
                          "/tank/scenario/external_data/nasa/daymet_v3/"]
              }

CATALOG_INPATH = {"public": {k: f"TDS/{v}/catalog.xml" for (k, v) in _tds_path.items()},
                  "private": _disk_path}

# Catalog output path
CATALOG_OUTPATH = os.environ.get('CATALOG_OUTPATH', default="/tmp/intake")
