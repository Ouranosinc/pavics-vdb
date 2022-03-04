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
from typing import Union, List
from pydantic import create_model, BaseModel, HttpUrl, constr, validator
from pydantic.dataclasses import dataclass
import re
from pathlib import Path
import cv
import datetime as dt

__all__ = ["CMIP5", "BiasAdjusted", "Reanalysis", "GridObs", "StationObs", "Forecast", "REGISTRY"]


REGISTRY = {"public": {}, "private": {}}


def register(name: str, kind="public"):
    """Register CV."""

    def _register(cls):
        REGISTRY[kind][name] = cls
        return cls

    return _register


class CV(BaseModel):
    """Controlled vocabulary used for public datasets listed on THREDDS.

    Common controlled vocabulary applying to all datasets.
    """
    variable_id: List[str]
    variable_name: List[str]
    variable_long_name: List[Union[str, None]]
    variable_standard_name: List[Union[str, None]]  # TODO: enforce standard_name CV

    @classmethod
    def attributes(cls):
        return list(cls.__fields__.keys())

    @classmethod
    def variable_attributes(cls):
        return [k for k in cls.attributes() if k.startswith("variable_")]

    @classmethod
    def dim_len_attributes(cls):
        return [k for k in cls.attributes() if k.startswith("dim_")]

    @classmethod
    def global_attributes(cls):
        other = cls.variable_attributes() + cls.dim_len_attributes()
        return [k for k in cls.attributes() if k not in other]


class PublicCV(CV):
    path: HttpUrl
    license_type: str
    time_coverage_start: Union[dt.datetime, None]
    time_coverage_end: Union[dt.datetime, None]


class PrivateCV(CV):
    path: Path
    license_type: Union[str, None]


_CMIP5 = create_model("_CMIP5", **cv.cv2enum(cv.load_cvs("CV/_cmip5")))


@register("cmip5")
class CMIP5(_CMIP5, PublicCV):
    """CMIP5 simulations

    References
    ----------
    CMIP5 CV: https://www.medcordex.eu/cmip5_data_reference_syntax.pdf
    """
    project_id: Literal["CMIP5"]
    institute_id: str
    model_id: str
    experiment_id: str
    parent_experiment_id: str
    frequency: cv.Frequency
    initialization_method: int
    physics_version: int
    realization: Union[int, None]
    dim_realization_len: int


_BiasAdjusted = create_model("_BiasAdjusted", **cv.cv2enum(cv.load_cvs("CV/biasadjusted")))


@register("biasadjusted")
class BiasAdjusted(_BiasAdjusted, PublicCV):
    """Bias adjusted projections."""
    dataset_id: str
    institute_id: str
    type: str
    processing: str
    project_id: str  # activity ?
    frequency: str
    modeling_realm: str
    driving_model_id: Union[str, None]
    driving_experiment_id: Union[str, None]
    driving_institute_id: Union[str, None]
    target_dataset_id: Union[str, None]
    target_institute_id: Union[str, None]




cmip5_dir_regex = "^\/(?:[^/]*/){3}(?P<project>\w*)\/" \
                  "(?P<product>\w*)\/" \
                  "(?P<institute>\w*)\/" \
                  "(?P<model>\w*)\/" \
                  "(?P<experiment>\w*)\/" \
                  "(?P<time_frequency>\w*)\/" \
                  "(?P<realm>\w*)\/" \
                  "(?P<cmip_table>\w*)\/" \
                  "(?P<ensemble>\w*)\/" \
                  "(?P<version>\w*)\/" \
                  "(?P<variable>\w*)"



@register("climex")
class Climex(PublicCV):
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
class Reanalysis(PublicCV):
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
class GridObs(PublicCV):
    """Gridded observations"""
    title: str
    institute_id: str
    institute: str
    frequency: str
    dataset_id: str
    version: Union[str, None]


@register("stationobs")
class StationObs(PublicCV):
    """station observations"""
    title: str
    institute_id: str
    institute: str
    frequency: str
    dataset_id: str


# I think we're missing some info here (e.g. name of forecast model).
@register("forecast")
class Forecast(PublicCV):
    """Weather forecasts"""
    title: str
    institution: str
    member: Union[int, None]
    #model: str

