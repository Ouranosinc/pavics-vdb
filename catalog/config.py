import os

TDS_ROOT = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/datasets/"

# Mapping of CV registered specs to TDS paths.
CATALOG_TDS_PATH = {"cmip5": "simulations/cmip5_multirun",
                    "biasadjusted": "simulations/bias_adjusted",
                    "climex": "simulations/climex",
                    "reanalysis": "reanalyses",
                    "gridobs": "gridded_obs",
                    "stationobs": "station_obs",
                    "forecast": "forecasts"}

# Catalog output path
CATALOG_OUTPATH = os.environ.get('CATALOG_OUTPATH', default="/tmp/intake")
