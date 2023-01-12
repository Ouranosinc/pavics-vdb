"""Base classes for metadata attributes data model and NcML parsers."""

from typing import Union, List, Any, Optional
from pydantic import BaseModel, HttpUrl, constr, validator
from pydantic.utils import GetterDict
from enum import Enum
import functools
import ncml
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


# --- Parsers ---

class NcMLParser(GetterDict):
    """This class is used to extract information from an NcML Element.

    Use this class by assigning it to the `getter_dict` attribute of pydantic models' `Config` class, along with
    `orm_mode=True`. It allows pydantic models to ingest an `lxml.etree.Element` using the `from_orm` method.

    Class attributes can either be a xpath expression string, or a function taking an `Element` input.

    Example
    -------
    class DataParser(NcMLParser):
      model = attribute("dataset_id")

    # Define the metadata validation logic
    class Data(BaseModel):
      title: str
      model: str

      class Config:
          orm_mode = True
          getter_dict = ForecastParser

    Forecast.from_orm(element)
    """
    _ns = ncml.NS
    _default = staticmethod(ncml.attribute)
    variables = staticmethod(ncml.get_variables)

    def xpath(self, expr, default):
        out = self._obj.xpath(expr, namespaces=self._ns)
        return out[0] if out else default

    def get(self, key, default):
        """Return XML element."""
        getter = getattr(self, key, self._default(key))

        if isinstance(getter, str):
            return self.xpath(getter, default)

        elif callable(getter):
            return getter(self._obj) or default

        else:
            raise ValueError


class PublicParser(NcMLParser):
    """Base parser for datasets hosted on THREDDS."""
    path = ncml.attribute("opendap_service")


class PrivateParser(NcMLParser):
    """Base parser for datasets stored on disk."""
    path = "@location"


class VariableParser(NcMLParser):
    """Parser for variable attributes."""
    name = "@name"
    _default = staticmethod(ncml.varattr)


# --- Data models ---

class CFVariable(BaseModel):
    """Data model for netCDF variable attributes."""

    name: str
    long_name: str
    standard_name: Optional[str]
    units: Optional[str]

    class Config:
        orm_mode = True
        getter_dict = VariableParser


class Common(BaseModel):
    """Common data model for netCDF dataset attributes.

    Note
    ----
    Do not create attributes that have the same name as internal `variables` attributes (name,
    standard_name, long_name, units).
    """
    variables: List[CFVariable]

    class Config:
        orm_mode = True
        getter_dict = NcMLParser

    @classmethod
    def global_attributes(cls):
        """Name of global attributes."""
        return [k for k in cls.__fields__.keys() if k != "variables"]

    @classmethod
    def variable_attributes(cls):
        """Name of variable attributes."""
        return list(cls.__fields__["variables"].type_.__fields__.keys())

    @classmethod
    def variable_attrs_name(cls):
        """Catalog names for variable attributes."""
        return [f"var_{att}" for att in cls.variable_attributes()]

    @classmethod
    def attributes(cls):
        return cls.global_attributes() + cls.variable_attributes()


class Public(Common):
    """Data model for THREDDS dataset attributes on public web server."""
    path: HttpUrl
    license_type: str
    time_coverage_start: dt.datetime
    time_coverage_end: dt.datetime

    class Config:
        orm_mode = True
        getter_dict = PublicParser


class Private(Common):
    """Data model for file-based dataset attributes on internal disks."""
    path: Path
    license_type: Union[str, None]

    class Config:
        orm_mode = True
        getter_dict = PrivateParser


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

