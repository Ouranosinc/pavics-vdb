from specs import CMIP5, BiasAdjusted, Reanalysis, GridObs, Forecast, REGISTRY

TDS_ROOT = "https://pavics.ouranos.ca/twitcher/ows/proxy/thredds/catalog/datasets/"

# Mapping of DRS registered specs to TDS paths.
CATALOG_TDS_PATH = {"cmip5": "simulations/cmip5_multirun",
                    "biasadjusted": "simulations/bias_adjusted",
                    "reanalysis": "reanalyses",
                    "gridobs": "gridded_obs",
                    "forecast": "forecasts"}

# Catalog output path
CATALOG_OUTPATH = "/tmp"


def create_intake_catalog(name, path):
    """Parse metadata from TDS catalog and write intake spec and csv to disk."""
    from intake_converter import Intake

    cls = REGISTRY[name]
    url = TDS_ROOT + CATALOG_TDS_PATH[name] + "/catalog.xml"

    spec = Intake(cls)
    table = spec.parse(url)
    spec.to_catalog(table, path)


def main():
    """Write all registered catalogs."""
    for name in REGISTRY.keys():
        create_intake_catalog(name, CATALOG_OUTPATH)


if __name__ == '__main__':
    main()



