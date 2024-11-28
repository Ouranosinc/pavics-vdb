"""
Gridded observations datasets catalog entry definition and validation rules.
"""
from .base import register, Attributes, Catalog



class GridObsAttributes(Attributes):
    """Data model for catalog entries for gridded observations datasets."""
    title: str
    institute_id: str
    institute: str
    frequency: str
    dataset_id: str
    version: str


@register("gridobs")
class GridObs(Catalog):
    attributes: GridObsAttributes
