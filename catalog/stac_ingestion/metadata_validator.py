import json
import jsonschema

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
        valid = False

        with open(schema_uri) as f:
            schema = json.load(f)

        try:
            jsonschema.validate(instance=item, schema=schema)
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