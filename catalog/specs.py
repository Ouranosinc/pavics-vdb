"""Catalogs specifications

:class:`CV` defines attributes common to all types of data.

Subclasses of CV define attributes for sub-categories. The attribute names should be defined in the global
attributes of each NcML file, except for attributes starting with `variable_`.

TODO: Clarify institute/institution for bias-adjusted
TODO: `type` is used for different purposes (GCM, reanalysis)
TODO: Include CV validation mechanism. Using `attrs` or `pydantic` ? The CV can probably be programmatically
      converted to a CV subclass.

"""
from typing_extensions import Literal
from pydantic import create_model, BaseModel
from pydantic.dataclasses import dataclass
import cv

__all__ = ["CMIP5", "BiasAdjusted", "Reanalysis", "GridObs", "StationObs", "Forecast", "REGISTRY"]


REGISTRY = {}


def register(name: str):
    """Register CV."""

    def _register(cls):
        REGISTRY[name] = cls
        return cls

    return _register


class CV(BaseModel):
    """Controlled vocabulary

    Common controlled vocabulary applying to all datasets.
    """
    license_type: str
    variable_name: list
    variable_long_name: list

    # Attributes that should be standard but may not be available everywhere.
    # version: str
    # variable_long_name: str
    # variable_standard_name: str
    # variable_units: str
    # time_coverage_start: dt.datetime
    # time_coverage_end: dt.datetime

    # Not implemented yet, but would include relationships to other datasets.
    # provenance: str = ""

    @classmethod
    def attributes(cls):
        return list(cls.__fields__.keys())

    @classmethod
    def global_attributes(cls):
        keys = [k for k in cls.attributes() if not k.startswith("variable_")]
        return keys

    @classmethod
    def variable_attributes(cls):
        return [k for k in cls.attributes() if k.startswith("variable_")]


_CMIP5 = create_model("_CMIP5", **cv.cv2enum(cv.load_cvs("../CV/_cmip5")))

@register("cmip5")
class CMIP5(_CMIP5, CV):
    """CMIP5 simulations

    References
    ----------
    CMIP5 CV: https://www.medcordex.eu/cmip5_data_reference_syntax.pdf
    """
    institute: str
    model: str
    experiment: str
    frequency: cv.Frequency
    mip_table: str
    ensemble_member: str
    version_number: str


_BiasAdjusted = create_model("_BiasAdjusted", **cv.cv2enum(cv.load_cvs("../CV/biasadjusted")))

@register("biasadjusted")
class BiasAdjusted(_BiasAdjusted, CV):
    """Bias adjusted projections."""
    title: str
    institution: str
    dataset_id: str
    driving_model: str
    driving_experiment: str
    institute: str
    type: str
    processing: str
    project_id: str  # activity ?
    frequency: str
    modeling_realm: str
    target_dataset: str
    driving_institution: str  # driving_institute ?
    driving_institute_id: str


_Cordex = create_model("_Climex", **cv.cv2enum(cv.load_cvs("../CV/cordex")))

class Cordex(_Cordex, CV):
    """
    References
    ----------
    https://is-enes-data.github.io/
    """
    driving_model_ensemble_member: cv.cmip5_member
    frequency: cv.CordexFrequency
    project_id: Literal["CORDEX"]


@register("climex")
class Climex(CV):
    """Bias adjusted projections."""
    title: str
    institution: str
    driving_model_id: str
    driving_experiment: str
    type: str
    processing: str
    project_id: str  # activity ?
    frequency: str
    modeling_realm: str


@register("reanalysis")
@dataclass
class Reanalysis(CV):
    """Reanalyses

    References
    ----------
    https://reanalyses.org/atmosphere/comparison-table
    """
    title: str
    institute_id: str
    institute: str
    dataset_id: str


    # assimilation_algorithm
    # resolution
    # areal_coverage
    # frequency (time averaging)


@register("gridobs")
@dataclass
class GridObs(CV):
    """Gridded observations"""
    title: str
    institute_id: str
    institute: str
    frequency: str
    dataset_id: str
    version: str


@register("stationobs")
@dataclass
class StationObs(CV):
    """station observations"""
    title: str
    institute_id: str
    institute: str
    frequency: str
    dataset_id: str


# I think we're missing some info here (e.g. name of forecast model).
@register("forecast")
@dataclass
class Forecast(CV):
    """Weather forecasts"""
    title: str
    institution: str
    member: int
    #model: str

