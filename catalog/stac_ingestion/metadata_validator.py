from pathlib import Path

import json
import jsonschema
import os
import logging
import sys


# setup logger
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.StreamHandler(sys.stdout))
LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False


class OBJECT_TYPE:
    ITEM = "ITEM"


# TODO : handle version range, http ref
REGISTERED_SCHEMAS = {
    "cmip5" : {
        OBJECT_TYPE.ITEM : "cv/cmip5/schema.json"
    }
}


def single_line(s):
    return str.join(" ", str(s).splitlines())


class MetadataValidator(object):
    def is_valid(self, item, schema_uri):
        # TODO : local path test
        HERE = Path(__file__).resolve().parent
        SCHEMA_DIR = HERE / "cv" / "cmip5"
        valid = False

        with open(schema_uri) as f:
            schema = json.load(f)

        try:
            resolver = jsonschema.RefResolver(base_uri=f"file://{SCHEMA_DIR}{os.path.sep}", referrer=schema)
            jsonschema.validate(instance=item, schema=schema, resolver=resolver)

            valid = True
        except jsonschema.exceptions.ValidationError as err:
            LOGGER.warning("[WARNING] validation error: %s", single_line(err))
        except jsonschema.exceptions.SchemaError as err:
            LOGGER.warning("[WARNING] schema error: %s", single_line(err))
        except jsonschema.exceptions.RefResolutionError as err:
            LOGGER.warning("[WARNING] ref resolution error: %s", single_line(err))
        except jsonschema.exceptions.UndefinedTypeCheck as err:
            LOGGER.warning("[WARNING] undefined type error: %s", single_line(err))
        except jsonschema.exceptions.UnknownType as err:
            LOGGER.warning("[WARNING] unknown type error=: %s", single_line(err))
        except jsonschema.exceptions.FormatError as err:
            LOGGER.warning("[WARNING] format error: %s", single_line(err))
        except jsonschema.ValidationError as err:
            LOGGER.warning("[WARNING] validation error: %s", single_line(err))

        return valid
