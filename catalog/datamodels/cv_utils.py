"""CV parsing utilities."""

import json
from enum import Enum
from pathlib import Path


def collection2enum(collection):
    """Create Enum based on terms from pyessv collection.

    Parameters
    ----------
    collection : pyessv.model.collection.Collection
      pyessv collection of terms.

    Returns
    -------
    Enum
      Enum storing terms and their labels from collection.
    """
    mp = {term.name: term.label for term in collection}
    return Enum(collection.raw_name.capitalize(), mp, module="base")


def cv2enum(name, cv):
    """Create Enum class from controlled vocabulary.

    Parameters
    ----------
    name : str
      Name of class to be created. Use camel case convention, e.g. MyDataModel.
    cv : dict, list
      Controlled vocabulary (key: label).

    Returns
    -------
    dict
      Enum subclass.

    Notes
    -----
    Sometimes the CV is just a flat list, e.g. CMIP6_nominal_resolution.json. In this case, use a Literal to describe
    this contraint to pydantic.

    Example
    -------
    >>> fields = cv2enum("Experiment", {"rcp45": "RCP4.5"})

    """
    # Typically, the CV is a dictionary. Note that in some cases, (e.g. CMIP6_experiment_id.json), the value is
    # another dict, in this case we just set the value to the attribute name.
    if isinstance(cv, dict):
        try:
            ival = {v: k for (k, v) in cv.items()}
        except TypeError:
            ival = {k: k for (k, v) in cv.items()}

    elif isinstance(cv, list):
            ival = {v: v for v in cv}

    else:
        raise ValueError(cv)

    return Enum(name, ival, module="base")


def load_cvs(path):
    """Return dictionary of controlled vocabularies stored as json files."""
    fns = Path(path).glob("*.json")

    cvs = {}
    for fn in fns:
        cvs.update(json.loads(fn.read_text()))
    cvs.pop("version_metadata", None)

    if len(cvs) == 0:
        raise FileNotFoundError(f"Not CV description found in {Path(path)}")

    return cvs

