import sys
sys.path.append(".")
from catalog.datamodels import REGISTRY
from catalog.config import TDS_ROOT, CATALOG_TDS_PATH, CATALOG_OUTPATH, LOGFILE
import click

collections = list(CATALOG_TDS_PATH.keys())

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
@click.option('-l', '--log', default=LOGFILE,
              show_default=True,
              help="Output path for log.")
def intake_cli(collection, output, log):
    for coll in collection:
        create_intake_catalog(coll, output, log)


def create_intake_catalog(collection, output, log):
    """Parse metadata from TDS catalog and write intake spec and csv to disk."""
    from catalog.intake_ingestion.intake_converter import Intake
    from catalog.tds import walk
    from loguru import logger
    import sys

    log_config = {
        "handlers": [
            {"sink": sys.stdout, "level": "WARNING"},
            {"sink": log, "level": "INFO"},
        ],
    }
    logger.configure(**log_config)
    logger.info(f"Creating `{collection}` catalog.")

    # Check that the different data models for a given collection are identical.
    dms = [REGISTRY[name] for name in CATALOG_TDS_PATH[collection].values()]
    if not unique_schemas(dms):
        raise ValueError(f"Datamodels schemas for {collection} are not identical.")

    cat = []
    for path, dm in CATALOG_TDS_PATH[collection].items():
        url = TDS_ROOT + path + "/catalog.xml"

        esmcat = Intake(cv=REGISTRY[dm])
        cat.extend(esmcat.catalog(walk(url)))

    logger.info(f"Saving catalog to disk at {output}")
    esmcat.save(cat, output)


def unique_schemas(dms):
    """Return True if datamodels have the same schema."""
    from dataclasses import fields
    schemas = [tuple(dm.schema().keys()) for dm in dms]
    return len(set(schemas)) == 1


if __name__ == '__main__':
    intake_cli()



