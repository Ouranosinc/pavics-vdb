from pydantic.dataclasses import create_model, dataclass
from pydantic import constr, BaseModel, validator
from enum import Enum
from pathlib import Path
import json
import functools
from typing import Dict

@functools.total_ordering
class OrderedEnum(Enum):
    """Ordered enumeration."""
    # From woodruffw/ordered_enum <william @ yossarian.net>

    @classmethod
    @functools.lru_cache(None)
    def _member_list(cls):
        return list(cls)

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            member_list = self.__class__._member_list()
            return member_list.index(self) < member_list.index(other)
        return NotImplemented


class Frequency(OrderedEnum):
    """Frequency attribute that supports inequality comparisons."""
    FX = "fx"
    HOUR = "1hr"
    HOUR3 = "3hr"
    HOUR6 = "6hr"
    DAY = "day"
    MONTH = "mon"
    YEAR = "yr"
    DEC = "dec"


class CordexFrequency(OrderedEnum):
    """Frequency attribute that supports inequality comparisons."""
    FX = "fx"
    HOUR3 = "3hr"
    HOUR6 = "6hr"
    DAY = "day"
    MONTH = "mon"
    SEM = "sem"

cmip5_member = constr(regex=r"^r\di\dp\d$")


class CFVariable(BaseModel):
    id: str  # Standard alias
    name: str  # Original variable name
    standard_name: str  # CF standard name
    long_name: str
    units: str

    def __init__(self, **data):
        data["id"] = data["name"]  # TODO: Fetch standard name
        super().__init__(**data)

    @validator("name")
    def not_a_coordinate(cls, v):
        if v in ["time", "lon", "lat", "rlon", "rlat", "height", "level", "longitude", "latitude"]:
            raise ValueError
        return v


def cv2enum(cvs):
    """Create Enum classes from controlled vocabularies.

    Parameters
    ----------
    name : str
      Name of class to be created. Use camel case convention, e.g. MyDataModel.
    cvs : dict
      Controlled vocabularies, keyed by id: {attribute_id: {"key": "Standard name", ...}}

    Returns
    -------
    dict
      Enum classes.

    Example
    -------
    >>> cvs = load_cvs(<path>)
    >>> fields = cv2enum(cvs)
    >>> create_model("MyClass", **fields)
    """
    enums = {}
    for key, val in cvs.items():

        # Sometimes the CV is just a flat list, e.g. CMIP6_nominal_resolution.json
        if isinstance(val, list):
            ival = {v: v for v in val}

        # Typically, the CV is a dictionary. Note that in some cases, (e.g. CMIP6_experiment_id.json), the value is
        # another dict, in this case we just set the value to the attribute name.
        elif isinstance(val, dict):
            try:
                ival = {v: k for (k, v) in val.items()}
            except TypeError:
                ival = {k: k for (k, v) in val.items()}

        else:
            raise ValueError(key, val)

        # Set default value for CVs with single value
        if len(ival) == 1:
            default = list(ival.values())[0]
        else:
            default = ...

        # Create Enum subclass
        enums[key] = (Enum(key.capitalize(), ival, module="spec2"), default)

    return enums


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
