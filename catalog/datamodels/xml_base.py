from pydantic.utils import GetterDict
from .. import ncml

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
