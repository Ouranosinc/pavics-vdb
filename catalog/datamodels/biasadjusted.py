"""
Bias-adjusted datasets catalog entry definition and validation rules.
"""
# https://docs.pydantic.dev/usage/types/#discriminated-unions-aka-tagged-unions

from .base import register, OrderedEnum, Attributes, Catalog
from .cmip5 import  Realm
from typing import Union
from pydantic import BaseModel, Field, constr


class Frequency(OrderedEnum):
    """Frequency attribute that supports inequality comparisons."""
    FX = "fx"
    HOUR = "1hr"
    HOUR3 = "3hr"
    HOUR6 = "6hr"
    DAY = "day"
    MONTH = "mon"
    YEAR = "yr"
    DEC = "10yr"

    NONSTD = "non-standard"  # non standard



class BiasAdjustedAttributes5(Attributes):
    """Data model for catalog entries for bias-adjusted datasets."""
    path_: constr(regex=".*/datasets/simulations/bias_adjusted/cmip5/*")
    activity: str = Field(..., alias="project_id")
    title: str
    institution: str
    dataset_id: str
    driving_model: str = Field(..., alias="driving_model_id")
    driving_experiment: str
    institute: str = Field(..., alias="institute_id")
    type: str
    processing: str
    bias_adjustment_method: str
    frequency: Frequency
    modeling_realm: Realm
    target_dataset: str
    driving_institution: str  # driving_institute ?
    driving_institute_id: str


class BiasAdjustedAttributes6(BiasAdjustedAttributes5):
    path_: constr(regex=".*/datasets/simulations/bias_adjusted/cmip6/*")
    driving_model: str = Field(..., alias="GCM__model_id")
    driving_experiment: str = Field(..., alias="GCM__experiment_id")
    institute: str = Field(..., alias="target__institution_id")
    target_dataset: str = Field(..., alias="target__dataset")
    driving_institution: str = Field(..., alias="GCM__institution")
    driving_institute_id: str = Field(..., alias="GCM__institution_id")


@register("biasadjusted")
class BiasAdjusted(Catalog):
    #attributes: BiasAdjusted5
    attributes: Union[BiasAdjustedAttributes5, BiasAdjustedAttributes6] = Field(..., discriminator="path_")
