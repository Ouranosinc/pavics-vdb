"""
Gridded observations datasets catalog entry definition and validation rules.
"""
from .base import register, Public


@register("gridobs")
class GridObs(Public):
    """Data model for catalog entries for gridded observations datasets."""
    title: str
    institute_id: str
    institute: str
    frequency: str
    dataset_id: str
    version: str
