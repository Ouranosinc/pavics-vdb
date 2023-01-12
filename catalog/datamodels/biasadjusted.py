"""
Bias-adjusted datasets catalog entry definition and validation rules.
"""

from .base import register, Public, PublicParser
from ncml import attribute
from .cmip5 import Frequency, Realm


class BAParser(PublicParser):
    """Map catalog entries to metadata attributes found in netCDF."""
    activity = attribute("project_id")
    driving_model = attribute("GCM__model_id")
    driving_experiment = attribute("GCM__experiment_id")
    institute = attribute("target__institution_id")
    target_dataset = attribute("target__dataset")
    driving_institution = attribute("GCM__institution")
    driving_institute_id = attribute("GCM__institution_id")


@register("biasadjusted")
class BiasAdjusted(Public):
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
        getter_dict = BAParser
