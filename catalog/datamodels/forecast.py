"""
Weather forecasts datasets catalog entry definition and validation rules.
"""

from .base import register, Public, PublicParser
from ..ncml import attribute, dimlen


class ForecastParser(PublicParser):
    """Map catalog entries to metadata attributes found in netCDF."""
    model = attribute("dataset_id")
    member = dimlen("member")


# I think we're missing some info here (e.g. name of forecast model).
@register("forecast")
class Forecast(Public):
    """Data model for catalog entries for numerical weather forecasts datasets."""
    title: str
    institution: str
    member: int
    model: str

    class Config:
        orm_mode = True
        getter_dict = ForecastParser

