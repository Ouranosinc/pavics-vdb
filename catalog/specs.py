"""Catalogs specifications

:class:`DRS` defines attributes common to all types of data.
Subclasses of DRS define attributes for sub-categories. The attribute names should be defined in the global
attributes of each NcML file, except for attributes starting with `variable_`.

TODO: Clarify institute/institution for bias-adjusted
TODO: `type` is used for different purposes (GCM, reanalysis)

"""
import warnings
from dataclasses import dataclass, fields, asdict, astuple


__all__ = ["CMIP5", "BiasAdjusted", "Reanalysis", "GridObs", "Forecast", "REGISTRY"]


# TODO: Include CV validation mechanism. Using `attrs` or `pydantic` ? The CV can probably be programmatically
#  converted to a DRS subclass.

REGISTRY = {}


def register(name: str):
    """Register DRS."""

    def _register(cls):
        REGISTRY[name] = cls
        return cls

    return _register


@dataclass
class DRS:
    """Data Reference Syntax

    Common controlled vocabulary applying to all datasets.
    """
    license_type: str
    variable_name: list
    variable_long_name: list

    @classmethod
    def attributes(cls):
        return [f.name for f in fields(cls)]

    @classmethod
    def global_attributes(cls):
        keys = [k for k in cls.attributes() if not k.startswith("variable_")]
        return keys

    @classmethod
    def variable_attributes(cls):
        return [k for k in cls.attributes() if k.startswith("variable_")]


@register("cmip5")
@dataclass
class CMIP5(DRS):
    """CMIP5 simulations

    References
    ----------
    CMIP5 DRS: https://www.medcordex.eu/cmip5_data_reference_syntax.pdf
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


@register("biasadjusted")
@dataclass
class BiasAdjusted(DRS):
    """Bias adjusted projections."""
    title: str
    institution: str
    dataset_id: str
    driving_model: str
    driving_experiment: str
    institute: str
    type: str
    processing: str
    bias_adjustment_method: str
    project_id: str  # activity ?
    frequency: str
    modeling_realm: str
    target_dataset: str
    driving_institution: str  # driving_institute ?
    driving_institute_id: str


@register("reanalysis")
@dataclass
class Reanalysis(DRS):
    """Reanalyses

    References
    ----------
    https://reanalyses.org/atmosphere/comparison-table
    """
    title: str
    institute_id: str
    dataset_id: str

    # assimilation_algorithm
    # resolution
    # areal_coverage
    # frequency (time averaging)


@register("gridobs")
@dataclass
class GridObs(DRS):
    title: str
    institute_id: str
    institute: str
    frequency: str
    dataset_id: str
    version: str


# I think we're missing some info here (e.g. name of forecast model).
@register("forecast")
@dataclass
class Forecast(DRS):
    institution: str
    member: int

