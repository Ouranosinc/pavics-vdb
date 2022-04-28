"""
CQ2 datasets catalog entry definition and validation rules.

Most of the catalog attributes are found within the NcML document, but some needs to be extracted from the file name.
The function `attributes_from_location` parses the location attribute from the NcML, them uses a pattern defined
below to map values to CV keys. New XML Elements are added to the NcML tree to include those additional attributes in
in the parser's init.

Notes
-----
The CQ2_CVs need to be accessible from ... ?

References
----------
CQ2 CV: https://github.com/Ouranosinc/CQ2_CVs
"""

from typing import Any, List
from typing_extensions import Literal
from .base import register, Public, PublicParser
from .cv_utils import load_cvs
from intake.source.utils import reverse_format
from .. import ncml
from lxml.etree import QName, SubElement, Element
from ..config import CVS_PATH
from pathlib import Path

cvs = load_cvs(Path(CVS_PATH) / "CQ2_CVs")

_cvs = cvs
pat = (
    r"{project_id}_{nature_id}_{spatial_domain_id}_{mapping_type_id}_{variable_id}_{period_id}_{ensemble_id}_{"
    r"gcm_id}_{rcm_id}_{rcp_id}_{post_processing_id}_{post_processing_obs_id}_{hydro_model_id}_{"
    r"hydro_calibration_id}_{hydro_calibration_obs_id}_{validation_contact_id}_{_trash}.nc"
)


class CQ2Parser(PublicParser):
    """Map catalog entries to metadata attributes in NcML description."""
    license = ncml.attribute("license")
    nature = ncml.attribute("nature_id")
    institute = ncml.attribute("institute_id")
    frequency = ncml.attribute("time_frequency")
    mapping_type = ncml.attribute("mapping_type_id")
    spatial_domain = ncml.attribute("spatial_domain_id")
    period = ncml.attribute("period_id")
    ensemble = ncml.attribute("ensemble_id")
    post_processing = ncml.attribute("post_processing_id")
    post_processing_obs = ncml.attribute("post_processing_obs_id")
    hydro_calibration = ncml.attribute("hydro_calibration_id")
    hydro_calibration_obs = ncml.attribute("hydro_calibration_obs_id")

    def __init__(self, obj: Any):
        attrs = attributes_from_location(obj, pat, cvs)
        obj.extend(attrs)
        super().__init__(obj)


def attributes_from_location(root: Element, pattern: str, cvs: dict) -> List[Element]:
    """Extract metadata from the file location pattern.

    Parameters
    ----------
    root : Element
      NcML etree.Element.
    pattern : str
      Formatting string that will be reversed to extract metadata.
    cvs : dict
      Controlled vocabulary mapping formatting pattern keys to dictionaries of controlled vocabularies.

    Returns
    -------
    list
      Sequence of `ncml:attribute` Elements.
    """
    location = root.xpath("/ncml:netcdf/@location", namespaces=ncml.NS)[0].split("/")[-1]

    # Reverse the format to find the key values
    d = reverse_format(pattern, location)

    # Extract the pretty values from the CVs
    meta = {key: cvs[key][val] for (key, val) in d.items() if not key.startswith("_")}

    # Append attributes to the XML
    out = []
    for name, value in meta.items():
        out.append(SubElement(root, QName(ncml.NS["ncml"], "attribute"), attrib=dict(name=name, value=value)))

    return out


@register("cq2")
class CQ2(Public):
    """Data model for catalog entries from CQ2 datasets."""
    license: str
    license_type = "permissive"
    activity: str
    project: str
    nature: str
    institute: Literal["DEH"]
    experiment_id: str
    cdm_data_type: str
    spatial_domain: str
    model_id: str
    version_id: str
    frequency: str
    driving_data: str
    rcm_model_id: str
    rcm_experiment_id: str
    rcm_domain: str
    rcm_ensemble: str
    rcm_driving_data: str
    gcm_model_id: str
    gcm_experiment_id: str
    gcm_ensemble: str
    #
    mapping_type: str
    period: str
    ensemble: str
    post_processing: str
    post_processing_obs: str
    hydro_calibration: str
    hydro_calibration_obs: str

    class Config:
        orm_mode = True
        getter_dict = CQ2Parser

