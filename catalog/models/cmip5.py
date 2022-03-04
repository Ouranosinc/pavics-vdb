from pydantic import create_model, ValidationError, BaseModel
from enum import Enum


class Activity(Enum):
    CMIP5 = "cmip5"

class Product:



class CMIP5(CV):
    """CMIP5 simulations

    References
    ----------
    CMIP5 CV: https://www.medcordex.eu/cmip5_data_reference_syntax.pdf
    """
    activity: str
    product: str
    institute: str
    model: str
    experiment: str
    frequency: str
    modeling_realm: str
    mip_table: str
    ensemble_member: str
    version_number: str
