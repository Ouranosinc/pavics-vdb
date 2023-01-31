"""
Reanalysis datasets catalog entry definition and validation rules.
"""

from .base import register, Attributes, Catalog



class ReanalysisAttributes(Attributes):
    """Data model for catalog entries for reanalysis datasets.

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
@register("reanalysis")
class Reanalysis(Catalog):
    attributes: ReanalysisAttributes
