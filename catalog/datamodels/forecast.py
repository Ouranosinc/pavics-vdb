"""
Weather forecasts datasets catalog entry definition and validation rules.
"""

from .base import register, Attributes, Catalog
from ..ncml import attribute, dimlen
from pydantic import Field



# I think we're missing some info here (e.g. name of forecast model).
class ForecastAttributes(Attributes):
    """Data model for catalog entries for numerical weather forecasts datasets."""
    title: str
    institution: str
    member: int
    model: str = Field(..., alias="dataset_id")


@register("forecast")
class Forecast(Catalog):
    attributes: ForecastAttributes

    def __init__(self, **kwargs):
        kwargs["attributes"]["member"] = kwargs["dimensions"]["member"]
        super().__init__(**kwargs)
