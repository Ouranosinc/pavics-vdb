from specs import REGISTRY
from config import CATALOG_OUTPATH, CATALOG_INPATH
import click

public_collections = list(REGISTRY["public"].keys())
private_collections = list(REGISTRY["private"].keys())

# Mathieu, je pense qu'on pourrait créer un click.group() pour ajouter une commande stac_cli.
# Voir https://click.palletsprojects.com/en/7.x/commands/


@click.command()
@click.option('-c', '--collection',
              default=public_collections,
              multiple=True,
              help=f"Name of dataset collection to catalog. One of {public_collections}, or all of them if None.")
@click.option('-o', '--output', default=CATALOG_OUTPATH,
              show_default=True,
              help="Output path for catalog files.")
@click.option('-k', '--kind', default="public",
              show_default=True,
              help="Catalog category {public|private}. Public catalogs fetch metadata from public THREDDS server, "
                   "while the private catalogs point to netCDF files on local disks.")
def intake_cli(collection, output, kind):
    for coll in collection:
        create_intake_catalog(coll, output, kind)


def create_intake_catalog(collection, output, kind):
    """Parse metadata from TDS catalog and write intake spec and csv to disk."""
    from intake_ingestion.intake_converter import Intake

    cls = REGISTRY[kind][collection]
    path = CATALOG_INPATH[kind][collection]

    spec = Intake(cls)
    table = spec.parse(path)
    spec.to_catalog(table, output)


if __name__ == '__main__':
    #intake_cli()
    create_intake_catalog('cordex', '/tmp/', 'private')




