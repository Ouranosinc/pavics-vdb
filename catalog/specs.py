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


_Cordex = create_model("_Climex", **cv.cv2enum(cv.load_cvs("CV/cordex")))

CORDEX_domain_pattern = r"^(SAM|CAM|NAM|EUR|AFR|WAS|EAS|CAS|AUS|ANT|ARC|AEC|MED|MNA)-(44|22|11|055|0275)$"
date_pattern = re.compile("^.*_(\d+)(?:-(\d+))?.nc$")

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

# http://is-enes-data.github.io/cordex_archive_specifications.pdf
cordex_dir_regex = "^\/(?:[^/]*/){3}(?P<project>\w*)\/" \
                  "(?P<institute>\w*)\/" \
                  "(?P<model>\w*)\/" \
                  "(?P<experiment>\w*)\/" \
                  "(?P<time_frequency>\w*)\/" \
                  "(?P<realm>\w*)\/" \
                  "(?P<cmip_table>\w*)\/" \
                  "(?P<ensemble>\w*)\/" \
                  "(?P<version>\w*)\/" \
                  "(?P<variable>\w*)"

cordex_fn_regex_1 = "^(?P<institute_id>\w*)\/.*\/" \
                    "(?P<VariableName>\w*)_" \
                    "(?P<Domain>\w*)_" \
                    "(?P<GCMMmodelName>\w*)_" \
                    "(?P<CMIP5ExperimentName>\w*)_" \
                    "(?P<CMIP5EnsembleMember>\w*)_" \
                    "(?P<RCMModelName>\w*)_" \
                    "(?P<RCMVersionID>\w*)_" \
                    "(?P<Frequency>\w*).nc"

cordex_fn_regex_2 = "^(?P<institute_id>\w*)\/.*\/" \
                    "(?P<VariableName>\w*)_" \
                    "(?P<Domain>\w*)_" \
                    "(?P<GCMMmodelName>\w*)_" \
                    "(?P<CMIP5ExperimentName>\w*)_" \
                    "(?P<CMIP5EnsembleMember>\w*)_" \
                    "(?P<RCMModelName>\w*)_" \
                    "(?P<RCMVersionID>\w*)_" \
                    "(?P<Frequency>\w*)_" \
                    "(?P<StartTime>\d+)-(?P<EndTime>\d+).nc"

cordex_paths = Union[constr(regex=cordex_fn_regex_1), constr(regex=cordex_fn_regex_2)]

'''
{variable_id}_{domain_id}_{driver_id}_{experiment_id}_{member_id}_{model_id}_{*}_{timestep_id}_r0i0p0.nc"
{variable_id}_{domain_id}_{driver_id}_{experiment_id}_{member_id}_{model_id}_{*}_{timestep_id}_{date_start}-{date_end}.nc"
{variable_id}_{domain_id}_{driver_id}_{experiment_id}_{member_id}_{model_id}_{*}_{timestep_id}.nc"
{variable_id}.{experiment_id}.{driver_id}.{model_id}.{timestep_id}.{domain_id}.raw.nc"
{variable_id}.{experiment_id}.{driver_id}.{model_id}.{timestep_id}.{domain_id}.raw.{*}.nc"  
{variable_id}.{experiment_id}.{driver_id}.{model_id}.{timestep_id}.{domain_id}.{date_start}.nc"
'''

def parse_dates(path):
    match = date_pattern.match(path)
    if match:
        start, end = match.groups()
        fmts = {8: "%Y%m%d", 6: "%Y%m"}

        def _parse(s, fmts):
            if s:
                return dt.datetime.strptime(s, fmts[len(s)])

        return _parse(start, fmts), _parse(end, fmts)

    else:
        return None, None


def test_parse_dates():
    path = ".../tasmin_NAM-44_CCCma-CanESM2_rcp85_r1i1p1_UQAM-CRCM5_v1_day_20960101-21001231.nc"
    s, e = parse_dates(path)
    assert s == dt.datetime(2096, 1, 1)
    assert e == dt.datetime(2100, 12, 31)

    path = ".../tasmin_NAM-44_CCCma-CanESM2_rcp85_r1i1p1_UQAM-CRCM5_v1_day.nc"
    s, e = parse_dates(path)
    assert s is None
    assert e is None

    path = ".../tasmin_NAM-44_CCCma-CanESM2_rcp85_r1i1p1_UQAM-CRCM5_v1_day_20960101.nc"
    s, e = parse_dates(path)
    assert s == dt.datetime(2096, 1, 1)
    assert e is None

    path = ".../tasmin_NAM-44_CCCma-CanESM2_rcp85_r1i1p1_UQAM-CRCM5_v1_day_209601-210012.nc"
    s, e = parse_dates(path)
    assert s == dt.datetime(2096, 1, 1)
    assert e == dt.datetime(2100, 12, 1)


class CordexPath_1(BaseModel):
    path: constr(regex="(variable_id}_{domain_id}_{driver_id}_{experiment_id}_{member_id}_{model_id}_{*}_{"
                       "timestep_id}_r0i0p0.nc")



@register("cordex", "private")
class Cordex(_Cordex, PrivateCV):
    """
    References
    ----------
    https://is-enes-data.github.io/
    """
    driving_model_ensemble_member: cv.cmip5_member
    frequency: Literal["fx", "3hr", "6hr", "day", "mon", "sem"]
    project_id: Literal["CORDEX"]
    CORDEX_domain: constr(regex=CORDEX_domain_pattern)

    domain_id: str  # Parsed from CORDEX_domain
    resolution_id: str  # Parsed from CORDEX_domain
    time_coverage_start: Union[dt.datetime, None]  # Parsed from path
    time_coverage_end: Union[dt.datetime, None]  # Parsed from path

    def __init__(self, **data):
        data["domain_id"], data["resolution_id"] = data["CORDEX_domain"].split("-")
        data["time_coverage_start"], data["time_coverage_end"] = parse_dates(data["path"])

        super().__init__(**data)


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

