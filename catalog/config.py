import os, sys

TDS_ROOT = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/"

# Mapping of CV registered specs to TDS paths.
# {collection: {path: datamodel}}
CATALOG_TDS_PATH = {"cmip5": {"datasets/simulations/cmip5_multirun": "cmip5"},
                    "biasadjusted": {#"simulations/bias_adjusted/cmip5/ouranos/cb-oura-1.0": "cb_oura_1_0",
                                     "birdhouse/testdata/test_ncmls": "biasadjusted5",
                                     "datasets/simulations/bias_adjusted/cmip6": "biasadjusted6",
                                     "datasets/simulations/bias_adjusted/cmip5": "biasadjusted5",},
                    "climex": {"datasets/simulations/climex": "climex"},
                    "reanalysis": {"datasets/reanalyses": "reanalysis"},
                    "gridobs": {"datasets/gridded_obs": "gridobs"},
                    "stationobs": {"datasets/station_obs": "stationobs"},
                    "forecast": {"datasets/forecasts": "forecast"}}

# Catalog output path
CATALOG_OUTPATH = os.environ.get('CATALOG_OUTPATH', default="/tmp/intake")

# Path to log file
LOGFILE = "/tmp/log/pavics_catalog.txt"

# Loguru configuration
log_config = {
    "handlers": [
        {"sink": sys.stdout, "level": "WARNING"},
        {"sink": LOGFILE, "level": "INFO"},
    ],
}
