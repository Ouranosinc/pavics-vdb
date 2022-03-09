"""
# Data models for catalog attributes.

Data models are based on pydantic dataclasses to validate entries, and avoid propagating metadata errors into catalogs.

Common classes are defined in `base.py`, while each individual data model is described in its own module.
"""
from .base import REGISTRY
from .cmip5 import CMIP5
from .biasadjusted import BiasAdjusted
from .climex import Climex
from .reanalysis import Reanalysis
from .gridobs import GridObs
from .stationobs import StationObs
from .forecast import Forecast
