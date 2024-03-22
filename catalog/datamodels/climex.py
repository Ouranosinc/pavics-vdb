"""
ClimEx datasets catalog entry definition and validation rules.
"""

from typing_extensions import Literal
from .base import register, Attributes, Catalog
from .cmip5 import Frequency, Realm


class ClimexAttributes(Attributes):
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


@register("climex")
class Climex(Catalog):
    attributes: ClimexAttributes
