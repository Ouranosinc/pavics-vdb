from typing_extensions import Literal
from typing import Union, List
from pydantic import create_model, BaseModel, HttpUrl, constr, validator
from pydantic.dataclasses import dataclass
import re
from pathlib import Path
import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
#from .. import cv
#from .. specs import register, PrivateCV

'''
{variable_id}_{domain_id}_{driver_id}_{experiment_id}_{member_id}_{model_id}_{*}_{timestep_id}_r0i0p0.nc"
{variable_id}_{domain_id}_{driver_id}_{experiment_id}_{member_id}_{model_id}_{*}_{timestep_id}_{date_start}-{date_end}.nc"
{variable_id}_{domain_id}_{driver_id}_{experiment_id}_{member_id}_{model_id}_{*}_{timestep_id}.nc"
{variable_id}.{experiment_id}.{driver_id}.{model_id}.{timestep_id}.{domain_id}.raw.nc"
{variable_id}.{experiment_id}.{driver_id}.{model_id}.{timestep_id}.{domain_id}.raw.{*}.nc"  
{variable_id}.{experiment_id}.{driver_id}.{model_id}.{timestep_id}.{domain_id}.{date_start}.nc"
'''

#_Cordex = create_model("_Climex", **cv.cv2enum(cv.load_cvs("CV/cordex")))

CORDEX_domain_pattern = r"^(SAM|CAM|NAM|EUR|AFR|WAS|EAS|CAS|AUS|ANT|ARC|AEC|MED|MNA)-(44|22|11|055|0275)$"
date_pattern = re.compile("^.*_(\d+)(?:-(\d+))?.nc$")

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


cordex_fn_regex_1 = "^(?P<institute_id>[a-zA-Z0-9-]+)\/.*\/" \
                    "(?P<VariableName>[a-zA-Z0-9-]+)_" \
                    "(?P<Domain>[a-zA-Z0-9-]+)_" \
                    "(?P<GCMModelName>[a-zA-Z0-9-]+)_" \
                    "(?P<CMIP5ExperimentName>[a-zA-Z0-9-]+)_" \
                    "(?P<CMIP5EnsembleMember>[a-zA-Z0-9-]+)_" \
                    "(?P<RCMModelName>[a-zA-Z0-9-]+)_" \
                    "(?P<RCMVersionID>[a-zA-Z0-9-]+)_" \
                    "(?P<Frequency>[a-zA-Z0-9-]+)(?:_" \
                    "(?P<StartTime>[\d-]+)-(?P<EndTime>[\d-]+))?.nc"


cordex_fn_regex_2 = "^(?P<institute_id>\w*)\/.*\/" \
                    "(?P<VariableName>\w*)_" \
                    "(?P<Domain>[-\w]*)_" \
                    "(?P<GCMModelName>[-\w]*)_" \
                    "(?P<CMIP5ExperimentName>\w*)_" \
                    "(?P<CMIP5EnsembleMember>\w*)_" \
                    "(?P<RCMModelName>[-\w]*)_" \
                    "(?P<RCMVersionID>\w*)_" \
                    "(?P<Frequency>\w*)_" \
                    "(?P<StartTime>\d+)-(?P<EndTime>\d+).nc"

cordex_paths = Union[constr(regex=cordex_fn_regex_1), constr(regex=cordex_fn_regex_2)]

class CordexPath(BaseModel):
    """CORDEX CV extracted from a file path."""
    _regex = cordex_fn_regex_1
    path: constr(regex=_regex)
    VariableName: str
    Domain: str
    GCMModelName: str
    CMIP5ExperimentName: str
    RCMModelName: str
    RCMVersionID: str
    Frequency: Literal["fx", "3hr", "6hr", "day", "mon", "sem"]
    StartTime: str = None
    EndTime: str = None
    time_coverage_start: Union[dt.datetime, None]  # Parsed from path
    time_coverage_end: Union[dt.datetime, None]  # Parsed from path

    def __init__(self, **data):
        attrs = re.compile(self._regex).match(data["path"]).groupdict()
        data.update(attrs)

        # Parse Domain
        data["domain_id"], data["resolution_id"] = data["Domain"].split("-")

        # Parse dates
        if data["StartTime"] is not None:
            data["time_coverage_start"] = parse_date(data["StartTime"])
        if data["EndTime"] is not None:
            data["time_coverage_end"] = parse_date(data["EndTime"], offset=True)
        super().__init__(**data)

"""
@validator("StartTime", pre=True)
def parse_date(cls, value):
    if value is not None:
        return parse_date(value)

"""

def parse_date(value, offset=False):
    fmts = {4: ["%Y"], 6: ["%Y%m"], 7: ["%Y-%m"], 8: ["%Y%m%d"], 10: ["%Y%m%d%H", "%Y-%m-%d"], 12: ["%Y%m%d%H%M"]}
    for fmt in fmts[len(value)]:
        try:
            out = dt.datetime.strptime(value, fmt)
        except ValueError:
            continue
    if offset:
        if "m" not in fmt:
            d = pd.tseries.offsets.YearEnd(1)
        elif "d" not in fmt:
            d = pd.tseries.offsets.MonthEnd(1)
        elif "H" not in fmt:
            d = pd.tseries.offsets.Day(1)
        else:
            d = pd.tseries.offsets.Hour(1)
        out = out + d
    return out


def add_offset(date, freq):
    if freq == "mon":
        pd.tseries.offsets.YearEnd(1)


c1 = CordexPath(path="UQAM/CRCM5/NAM-44_ECMWF-ERAINT_evaluation/fx/atmos/r1i1p1/sftlf/sftlf_NAM-44_ECMWF"
                 "-ERAINT_evaluation_r1i1p1_UQAM-CRCM5_v1_fx.nc")

c2 = CordexPath(path="UQAM/CRCM5/NAM-44_ECMWF-ERAINT_evaluation/day/atmos/r1i1p1/snw/snw_NAM-44_ECMWF"
                    "-ERAINT_evaluation_r1i1p1_UQAM-CRCM5_v1_day_19960101-20001231.nc")


def f():
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
