"""
Station observation datasets catalog entry definition and validation rules.
"""

from .base import register, Attributes, Catalog



class StationObsAttributes(Attributes):
    """Data model for catalog entries for station observation datasets."""
    title: str
    institute_id: str
    institute: str
    frequency: str
    dataset_id: str


@register("stationobs")
class StationObs(Catalog):
    attributes: StationObsAttributes
