"""NcML parsing utilities."""
from lxml.etree import XMLParser, fromstring, Element

# NcML namespace
NS = {"ncml": "http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"}


def to_element(content: bytes) -> Element:
    """Parse NcML file into XML node."""

    # Parse XML content - UTF-8 encoded documents need to be read as bytes
    parser = XMLParser(encoding='UTF-8')
    return fromstring(content, parser=parser)


def attribute(name: str) -> str:
    """Return xpath expression for global NcML attributes."""
    return f"//ncml:attribute[@name='{name}']/@value"


def varattr(name: str) -> str:
    """Return xpath expression for NcML variable attributes."""
    return f"./ncml:attribute[@name='{name}']/@value"


def dimlen(name: str) -> str:
    """Return xpath expression for NcML dimension length"""
    return f"./ncml:dimension[@name='{name}']/@length"


def get_variables(elem: Element) -> Element:
    """Return <variable> nodes that are not coordinates.

    Parameters
    ----------
    elem : lxml.etree.Element
      <ncml:netcdf> element.
    """

    # Get bounds
    bexpr = "./ncml:variable[ncml:attribute[@name='_CoordinateAxisType']]/ncml:attribute[@name='bounds']/@value"
    bounds = elem.xpath(bexpr, namespaces=NS)

    # Filter variables that are not coordinates
    vexpr = "./ncml:variable[not(ncml:attribute[@name='_CoordinateAxisType'])]"
    elements = elem.xpath(vexpr, namespaces=NS)

    # Get dimension names
    dexpr = "./ncml:dimension/@name"
    dimensions = elem.xpath(dexpr, namespaces=NS)

    exclude = bounds + dimensions
    return [el for el in elements if el.xpath("@name")[0] not in exclude]
