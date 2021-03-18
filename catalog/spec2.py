"""
Experimental code meant to investigate use of pydantic class to
1. define data models for metadata
2. strictly validate incoming metadata against data model
3. generate data model schema
"""

from pydantic import create_model
from enum import Enum
from pathlib import Path
import json


def model_from_cvs(name, cvs):
    """Create a data model from controlled vocabularies.

    Parameters
    ----------
    name : str
      Name of class to be created. Use camel case convention, e.g. MyDataModel.
    cvs : dict
      Controlled vocabularies, keyed by id.

    Returns
    -------
    class
      pydantic.BaseModel subclass.
    """
    # Create Enums
    enums = {}
    for key, val in cvs.items():

        # Invert val dict
        ival = {v: k for (k, v) in val.items()}

        # Set default value for CVs with single value
        if len(ival) == 1:
            default = list(ival.values())[0]
        else:
            default = ...

        # Create Enum subclass
        enums[key] = (Enum(key.capitalize(), ival, module="spec2"), default)

    # Create pydantic BaseModel subclass with fields set to Enums.
    return create_model(name, **enums)


def load_cvs(path):
    """Return dictionary of controlled vocabularies stored as json files."""
    fns = Path(path).glob("*.json")
    cvs = {}
    for fn in fns:
        cvs.update(json.loads(fn.read_text()))
    return cvs


def test():
    cvs = load_cvs("CV/_cmip5")
    M = model_from_cvs("CMIP5", cvs)
    m = M(realm="aerosol")
    return M.schema()


