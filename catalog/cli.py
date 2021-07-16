from specs import REGISTRY
from config import TDS_ROOT, CATALOG_TDS_PATH, CATALOG_OUTPATH
import click

collections = list(REGISTRY.keys())

# Mathieu, je pense qu'on pourrait créer un click.group() pour ajouter une commande stac_cli.
# Voir https://click.palletsprojects.com/en/7.x/commands/


@click.command()
@click.option('-c', '--collection',
              default=collections,
              multiple=True,
              help=f"Name of dataset collection to catalog. One of {collections}, or all of them if None.")
@click.option('-o', '--output', default=CATALOG_OUTPATH,
              show_default=True,
              help="Output path for catalog files.")
def intake_cli(collection, output):
    for coll in collection:
        create_intake_catalog(coll, output)


def create_intake_catalog(collection, output):
    """Parse metadata from TDS catalog and write intake spec and csv to disk."""
    from intake_ingestion.intake_converter import Intake

    cls = REGISTRY[collection]
    url = TDS_ROOT + CATALOG_TDS_PATH[collection] + "/catalog.xml"

    spec = Intake(cls)
    table = spec.parse(url)
    spec.to_catalog(table, output)


if __name__ == '__main__':
    intake_cli()



