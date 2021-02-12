from specs import REGISTRY
from config import TDS_ROOT, CATALOG_TDS_PATH, CATALOG_OUTPATH
import click

collections = list(CATALOG_TDS_PATH.keys())


@click.command()
@click.option('--collection', default=None,
              help=f"Name of dataset collection to catalog. One of {collections}, or all of them if None.")
@click.option('--output', default=CATALOG_OUTPATH, help="Output path for catalog files.")
def create_intake_catalog(collection, output):
    """Parse metadata from TDS catalog and write intake spec and csv to disk."""
    from intake_ingestion.intake_converter import Intake

    if collection is None:
        for collection in REGISTRY.keys():
            create_intake_catalog(collection, output)

    cls = REGISTRY[collection]
    url = TDS_ROOT + CATALOG_TDS_PATH[collection] + "/catalog.xml"

    spec = Intake(cls)
    table = spec.parse(url)
    spec.to_catalog(table, output)


if __name__ == '__main__':
    create_intake_catalog()



