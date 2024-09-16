"""
CMIP5 datasets catalog entry definition and validation rules.

We check the validity of metadata attributes using pyessv's controlled vocabulary (CV).

References
----------
pyessv-archive: https://github.com/ES-DOC/pyessv-archive
CMIP5 CV: https://www.medcordex.eu/cmip5_data_reference_syntax.pdf
"""

from typing import Union, List, Dict
import re
from .base import register, OrderedEnum, CFVariable, Attributes, Catalog
from ..ncml import attribute
from .cv_utils import collection2enum
from pydantic import constr, validator, Field
import pyessv
from typing_extensions import Literal


# CMIP5 controlled vocabulary (CV)
CV = pyessv.WCRP.CMIP5


class Frequency(OrderedEnum):
    """Frequency attribute that supports inequality comparisons."""
    FX = "fx"
    HOUR = "1hr"
    HOUR3 = "3hr"
    HOUR6 = "6hr"
    DAY = "day"
    MONTH = "mon"
    YEAR = "yr"
    DEC = "dec"


# Enum classes built from the pyessv' CV
Variable = collection2enum(CV.variable)
Institute = collection2enum(CV.institute)
Model = collection2enum(CV.model)
Experiment = collection2enum(CV.experiment)
Realm = collection2enum(CV.realm)


class CFVariable5(CFVariable):
    """CMIP5 variables with CV constraint."""
    name: Variable


class CMIP5Attributes(Attributes):
    """Data model for catalog entries for CMIP5 simulations.
    """
    activity: Literal["CMIP5"] = Field("CMIP5", alias="project_id")
    product: Literal["output"] = "output"
    institute: Institute = Field(..., alias="institute_id")
    model: Model = Field(..., alias="model_id")
    experiment_id: Experiment
    experiment: Literal["RCP2.6", "RCP4.5", "RCP6.0", "RCP8.5"]
    frequency: Frequency
    modeling_realm: Realm
    mip_table: str = Field(..., alias="table_id")
    ensemble_member: Union[constr(regex=CV.ensemble.term_regex), None]
    version_number: str = None

    @validator("mip_table")
    def parse_mip_table(cls, v):
        """In CMIP5, the table_id attribute is sometimes prefixed by Table. This validator searches for valid
        terms in the string and returns a clean version."""
        try:
            collection2enum(CV["cmor-table"])(v)
            return v
        except ValueError as err:
            names = [term.name for term in CV["cmor-table"].terms]
            pat = re.compile(f"({'|'.join(names)})")
            match = pat.search(v)
            if match:
                return match.group()
            raise err


@register("cmip5")
class CMIP5(Catalog):
    attributes: CMIP5Attributes
    variables: Dict[str, CFVariable5]
