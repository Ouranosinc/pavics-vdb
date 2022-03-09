"""
ClimEx datasets catalog entry definition and validation rules.
"""

from typing_extensions import Literal
from .base import register, Public
from .cmip5 import Frequency, Realm


@register("climex")
class Climex(Public):
    """Data model for catalog entries for ClimEx datasets."""
    activity: Literal["ClimEx"] = "ClimEx"
    title: str
    institution: str
    driving_model_id: str
    driving_experiment: str
    type: str
    processing: str
    frequency: Frequency
    modeling_realm: Realm

