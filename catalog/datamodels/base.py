"""Base classes for metadata attributes data model and NcML parsers."""

from typing import Union, List, Any, Optional, Dict
from collections import OrderedDict
from pydantic import BaseModel, HttpUrl, constr, validator

from enum import Enum
import functools

from pathlib import Path
import datetime as dt


# Global data models registry
REGISTRY = {}


def register(name: str):
    """Add controlled vocabulary to global registry"""

    def _register(cls):
        REGISTRY[name] = cls
        return cls

    return _register


# --- Data models ---

class Attributes(BaseModel):
    """Should be extended for each collection."""
    path_: HttpUrl
    license_type: str
    time_coverage_start: dt.datetime
    time_coverage_end: dt.datetime


class CFVariable(BaseModel):
    """Data model for netCDF variable attributes."""

    name: str
    long_name: Optional[str]  # Some variables (e.g. grid mappings do not require this)
    standard_name: Optional[str]
    units: Optional[str]


class Catalog(BaseModel):
    attributes: Attributes
    variables: Dict[str, CFVariable]

    def __init__(self, **kwargs):
        # Copy attributes that are deeply nested within groups.
        if "THREDDSMetadata" in kwargs["groups"]:
            kwargs["attributes"]["path_"] = kwargs["groups"]["THREDDSMetadata"]["groups"]["services"]["attributes"]["opendap_service"]
            kwargs["attributes"]["time_coverage_start"] = kwargs["groups"]["CFMetadata"]["attributes"][
                "time_coverage_start"]
            kwargs["attributes"]["time_coverage_end"] = kwargs["groups"]["CFMetadata"]["attributes"]["time_coverage_end"]
        else:
            kwargs["attributes"]["path_"] = kwargs["@location"]

        # Ingest data variables only.
        variables = OrderedDict()
        bounds = [v.get("attributes", {}).get("bounds") for v in kwargs["variables"].values()]

        for name, var in kwargs["variables"].items():
            if '_CoordinateAxisType' not in var.get("attributes", {}) and name != var["shape"] and name not in bounds:
                variables[name] = var
                variables[name]["name"] = name

        kwargs["variables"] = variables

        super().__init__(**kwargs)



    @classmethod
    def _get_variable_attrs_name(cls):
        """Catalog names for variable attributes."""
        return [f"var_{att}" for att in cls.variable_attributes()]



# --- Classes for custom attribute specifications ---

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
        return

