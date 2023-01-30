"""
Bias-adjusted datasets catalog entry definition and validation rules.
"""

from .base import register, Public, PublicParser, OrderedEnum, BaseModel
from ..ncml import attribute
from .cmip5 import Frequency, Realm


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


class BAParser5(PublicParser):
    """Map catalog entries to metadata attributes found in netCDF."""
    activity = attribute("project_id")
    institute = attribute("institute_id")
    driving_model = attribute("driving_model_id")


class BAOuraParser5(PublicParser):
    activity = attribute("project_id")
    institute = attribute("institute_id")


class BAParser6(PublicParser):
    """Map catalog entries to metadata attributes found in netCDF."""
    activity = attribute("project_id")
    driving_model = attribute("GCM__model_id")
    driving_experiment = attribute("GCM__experiment_id")
    institute = attribute("target__institution_id")
    target_dataset = attribute("target__dataset")
    driving_institution = attribute("GCM__institution")
    driving_institute_id = attribute("GCM__institution_id")


@register("biasadjusted5")
class BiasAdjusted5(Public):
    """Data model for catalog entries for bias-adjusted datasets."""
    activity: str
    title: str
    institution: str
    dataset_id: str
    driving_model: str
    driving_experiment: str
    institute: str
    type: str
    processing: str
    bias_adjustment_method: str
    frequency: Frequency
    modeling_realm: Realm
    target_dataset: str
    driving_institution: str  # driving_institute ?
    driving_institute_id: str

    class Config:
        orm_mode = True
        getter_dict = BAParser5


@register("cb_oura_1_0")
class BiasAdjusted6(BiasAdjusted5):
    class Config:
        orm_mode = True
        getter_dict = BAOuraParser5


@register("biasadjusted6")
class BiasAdjusted6(BiasAdjusted5):
    class Config:
        orm_mode = True
        getter_dict = BAParser6


