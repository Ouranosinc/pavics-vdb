"""
Experimental code meant to investigate use of pydantic class to
1. define data models for metadata
2. strictly validate incoming metadata against data model
3. generate data model schema

Data models can be created automatically from CVs, or hand-crafted in Python.

I looked and it doesn't seem there are mature libraries to convert a schema into classes dynamically. There is a
datamodel_code_generator library (https://koxudaxi.github.io/datamodel-code-generator/) that outputs static python
code, but I didn't understand it could be used dynamically. The pipeline would thus be:
1. write/update schema
2. convert schema to python data model using `datamodel_codegen`
3. import schema to parse & validate metadata

If step 2 requires hand-tuning, this is not so great however.
"""

from pydantic import create_model, ValidationError
from enum import Enum
from pathlib import Path
import json
import pytest


def model_from_cvs(name, cvs):
    """Create a data model from existing controlled vocabularies.

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
        if isinstance(val, list):
            ival = {v: v for v in val}

        elif isinstance(val, dict):
            try:
                ival = {v: k for (k, v) in val.items()}
            except TypeError:
                ival = {k: k for (k, v) in val.items()}

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


def test_model_from_cvs():
    cvs = load_cvs("CV/_cmip5")
    M = model_from_cvs("CMIP5", cvs)

    # Check that this validates
    M(realm="aerosol")

    # Check that this does not validate
    with pytest.raises(ValidationError):
        M(realm="wrong!")


def cmip6_schema(path="../../CMIP6_CVs"):
    """Create a jsonchema for CMIP6 metadata.

    Based on the CV hosted at https://github.com/WCRP-CMIP/CMIP6_CVs


    Notes
    -----
    $ git clone https://github.com/WCRP-CMIP/CMIP6_CVs <local_path>

    Run this function in python with the path to the CMIP6 CV directory.
    """
    cvs = load_cvs(path)
    return model_from_cvs("CMIP6", cvs).schema()


