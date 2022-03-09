"""
Station observation datasets catalog entry definition and validation rules.
"""

from .base import register, Public


@register("stationobs")
class StationObs(Public):
    """Data model for catalog entries for station observation datasets."""
    title: str
    institute_id: str
    institute: str
    frequency: str
    dataset_id: str
