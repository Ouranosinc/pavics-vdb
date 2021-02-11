"""Catalogs specifications

:class:`DRS` defines attributes common to all types of data.
Subclasses of DRS define attributes for sub-categories. The attribute names should be defined in the global
attributes of each NcML file, except for attributes starting with `variable_`.

TODO: Clarify institute/institution for bias-adjusted
TODO: `type` is used for different purposes (GCM, reanalysis)
"""
from dataclasses import dataclass, fields, asdict, astuple
import datetime as dt


__all__ = ["CMIP5", "BiasAdjusted", "Reanalysis", "GridObs", "Forecast"]


esmcat_version = "0.1.0"

@dataclass
class DRS:
    """Data Reference Syntax

    Common controlled vocabulary applying to all datasets.
    """
    _asset_column_name = "path"
    _asset_format = "netcdf"

    path: str
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
    def to_intake_spec(cls):
        """Create Intake specification file."""

        attributes = [{"column_name": key} for key in cls.global_attributes() + cls.variable_attributes()]
        spec = {"esmcat_version": esmcat_version,
                "id": cls.__name__.lower(),
                "description": cls.__doc__.splitlines()[0],
                "catalog_file": cls.catalog_fn(),
                "attributes": attributes,
                "assets": {
                    "column_name": cls._asset_column_name,
                    "format": cls._asset_format
                }
                }
        return spec

    @classmethod
    def cid(cls):
        return cls.__name__.lower()

    @classmethod
    def catalog_fn(cls):
        return f"{cls.cid()}.csv"

    @classmethod
    def attributes(cls):
        return [f.name for f in fields(cls)]

    @classmethod
    def header(cls):
        """Return attribute table header names."""
        return cls.global_attributes() + cls.variable_attributes() + [cls._asset_column_name]

    @classmethod
    def global_attributes(cls):
        keys = [k for k in cls.attributes() if not k.startswith("variable_")]
        keys.remove(cls._asset_column_name)
        return keys

    @classmethod
    def asset_attribute(cls):
        return cls._asset_column_name

    @classmethod
    def variable_attributes(cls):
        return [k for k in cls.attributes() if k.startswith("variable_")]

    def aslist(self):
        """Return tuple of values."""
        d = asdict(self)
        return [d[k] for k in self.header()]


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


@dataclass
class GridObs(DRS):
    title: str
    institute_id: str
    institute: str
    frequency: str
    dataset_id: str
    version: str


# I think we're missing some info here (e.g. name of forecast model).
@dataclass
class Forecast(DRS):
    institution: str
    member: int

