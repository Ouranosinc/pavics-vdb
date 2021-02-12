from specs import REGISTRY
from config import TDS_ROOT, CATALOG_TDS_PATH, CATALOG_OUTPATH


def create_intake_catalog(name, path):
    """Parse metadata from TDS catalog and write intake spec and csv to disk."""
    from intake_ingestion.intake_converter import Intake

    cls = REGISTRY[name]
    url = TDS_ROOT + CATALOG_TDS_PATH[name] + "/catalog.xml"

    spec = Intake(cls)
    table = spec.parse(url)
    spec.to_catalog(table, path)


# Long, would you prefer a cli.py file with a click command ? There will also be a catalog generator using STAC.
def main():
    """Write all registered catalogs."""
    for name in REGISTRY.keys():
        create_intake_catalog(name, CATALOG_OUTPATH)


if __name__ == '__main__':
    main()



