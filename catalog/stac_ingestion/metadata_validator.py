from pathlib import Path

import json
import jsonschema
import os


class OBJECT_TYPE:
    ITEM = "ITEM"

# TODO : handle version range, http ref
REGISTERED_SCHEMAS = {
    "cmip5" : {
        OBJECT_TYPE.ITEM : "cv/cmip5/schema.json"
    }
}


class MetadataValidator(object):
    def validate(self, item, schema_uri):
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
            print("[WARNING] validation error")
        except jsonschema.exceptions.SchemaError as err:
            print("[WARNING] schema error")
        except jsonschema.exceptions.RefResolutionError as err:
            print("[WARNING] ref resolution error")
        except jsonschema.exceptions.UndefinedTypeCheck as err:
            print("[WARNING] undefined type error")
        except jsonschema.exceptions.UnknownType as err:
            print("[WARNING] unknown type error")
        except jsonschema.exceptions.FormatError as err:
            print("[WARNING] format error")
        except jsonschema.ValidationError as err:
            print("[WARNING] validation error")

        return valid