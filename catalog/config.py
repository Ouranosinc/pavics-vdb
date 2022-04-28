import os, sys
from pathlib import Path

TDS_ROOT = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/"

# Mapping of CV registered specs to TDS paths.
CATALOG_TDS_PATH = {"cmip5": "datasets/simulations/cmip5_multirun",
                    "biasadjusted": "datasets/simulations/bias_adjusted",
                    "climex": "datasets/simulations/climex",
                    "reanalysis": "datasets/reanalyses",
                    "gridobs": "datasets/gridded_obs",
                    "stationobs": "datasets/station_obs",
                    "forecast": "datasets/forecasts",
                    "cq2": "birdhouse/mddelcc/PROJECTIONS_HYDROCLIMATIQUES"}

# Catalog output path
CATALOG_OUTPATH = os.environ.get('CATALOG_OUTPATH', default="/tmp/intake")

# Controlled Vocabulary directory
CVS_PATH = os.environ.get("CVS_PATH", default=Path("~/src/").expanduser())

# Path to log file
LOGFILE = "/tmp/log/pavics_catalog.txt"

# Loguru configuration
log_config = {
    "handlers": [
        {"sink": sys.stdout, "level": "WARNING"},
        {"sink": LOGFILE, "level": "INFO"},
    ],
}
