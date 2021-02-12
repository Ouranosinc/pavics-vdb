TDS_ROOT = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/datasets/"

# Mapping of DRS registered specs to TDS paths.
CATALOG_TDS_PATH = {"cmip5": "simulations/cmip5_multirun",
                    "biasadjusted": "simulations/bias_adjusted",
                    "reanalysis": "reanalyses",
                    "gridobs": "gridded_obs",
                    "forecast": "forecasts"}

# Catalog output path
CATALOG_OUTPATH = "/tmp"
