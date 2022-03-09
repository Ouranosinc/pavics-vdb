# PAVICS Catalog

We want catalogs to access datasets used in the production of climate services. We have different kinds of datasets, climate simulations, reanalyses, gridded and at-site observations, bias-corrected projections, etc. These datasets are typically described by different attributes. It thus makes sense to create independent catalog entries, and leave it to users to select the right catalog. 

## Attributes and controlled vocabulary
In a catalog, each dataset is described by a set of attributes, forming a catalog entry. Based on consultations, it appears useful to include in those entries descriptive fields that go beyond simple dataset identification. That is, entries such as model resolution can be useful to filter datasets. 

To ensure catalog clarity, it is important that attributes are described as much as possible by a well-defined controlled vocabulary (CV). A CV describes the attributes and the possible values it can take. Validating values ensures that search queries are not sabotaged by minor typos or naming differences.   


## Implementation 

### Data model

Each catalog type (e.g. reanalysis) defines a _data model_, a representation of the attributes that make up a catalog entry. This data model is defined using pydantic, which makes it easy to impose rules and restrictions to the values entered in the catalog. It is possible, for example,  to constrain an attribute to a type, a list of possibilities or a regular expression. 

The definition of CVs for attributes uses `pyessv`, the library underlying metadata validation in ES-DOC. The `pyessv-archive` repository is cloned locally and loaded by pyessv at import time. pyessv defines a naming hierarchy: authority, scope, collection, term. Within a data model, each attribute bound by a CV is typed as an `Enum`, with a list of _terms_ taken from a pyessv _collection_, for example:

```python
import pyessv
from datamodels.cv_utils import collection2enum
from typing_extensions import Literal
from pydantic import BaseModel, HttpUrl

# Define the scope for the CV
CV = pyessv.WCRP.CMIP5

# Convert all the terms in the collection `realm` to an Enum. 
Realm = collection2enum(CV.realm) 

# Define a trivial data model
class CMIP5(BaseModel):
    """Data model for catalog entries for CMIP5 simulations.
    """
    path: HttpUrl
    activity: Literal["CMIP5"] = "CMIP5"
    modeling_realm: Realm
```

### Metadata extraction

To streamline metadata extraction, we're using NcML. NcML is an XML schema describing netCDF metadata; it includes global attributes, variable attributes and shape information. THREDDS also includes additional metadata: 
 - `CFMetadata`: bounding box information (e.g. `geospatial_lon_min`), time coverage (e.g. `time_coverage_start`),
 - `NCISOMetadata`: creation data and `nciso` version,
 - `THREDDSMetadata`: list of services offered for the dataset (e.g. `opendap_service`)
For file system datasets, the `ncks` utility `ncdump -hx <file path>` generates a NcML description. In both case, we parse the NcML file using the `lxml.etree` library to create an `Element` instance. 
 
To create catalog entries, we need a mapping between the `<ncml:netcdf>` content Element and data model attributes. We might, for example, want to map the `project_id` global attribute to an `activity` catalog entry. The initial version of this code parsed the XML and returned a dictionary of all attributes. Attributes belonging to the data model were then extracted, accounting for _alternate_ names. The current version rather connects the data model directly to the XML Element. pydantic supports conversion from a custom class instance to a data model, using the `from_orm` method. To take advantage of this, we create a class with a `get` method, which defines how to extract a given `key` from the Element class. The data model is then configured with
```python
 class Config:
  orm_mode = True
  getter_dict = CMIP5Parser
```
where `CMIP5Parser` is a subclass of `datamodels.base.NcMLParser`. This parser is instantiated by pydantic with the NcML Element instance, and a special `get` method uses `xpath` expressions to parse the XML and return the desired attributes. For example, to get the `source` attribute, the xpath expression would be `//ncml:attribute[@name='source']/@value`. The `NcMLParser` class assumes that if an explicit xpath expression is not given, it should use a default xpath expression generator `self._default(key)` to create one. A data model whose attributes are simply global attributes can then use `NcMLParser` directly as its `getter_dict`. If a custom mapping is necessary, then defining a subclass with fields set to xpath expressions allows for more flexibility:

```python
class CMIP5Parser(NcMLParser):
    """Map catalog entries to metadata attribute found in netCDF."""
    path = "//ncml:attribute[@name='opendap_service']/@value"
    activity = "//ncml:attribute[@name='project_id']/@value"
```

Helper functions (`attribute`, `dimlen`, `varattr`) are defined to facilitate the creation of these xpath expressions. 

A data model inheriting from `pydantic.BaseModel` and configured with `NcMLParser` or a subclass can be instantiated using `Model.from_orm(elem)`, where elem is an `Element` instance. 

This setup works well for global attributes but requires some adjustments for variable attributes. Indeed, a catalog entry for a dataset should list the data variables available, possibly their `long_name` and `standard_name`. Since we want to filter out coordinate variables, we cannot parse variable attributes individually. In other words, we need to extract available variables, filter them to remove coordinates, then export the metadata fields required by the catalog entry. See the `VariableParser` and `CFVariable` classes in `datamodels.base` to see how it's done. Note that the `NcMLParser` class has a special case for a data model field called `variables`. Instead of returning a string value, `NcMLParser.get("variables")` returns a list of variable node Elements, which are then parsed individually using 
```python
class Common(BaseModel):
    """Common data model for netCDF dataset attributes.
    variables: List[CFVariable]
    
    class Config:
        orm_mode = True
        getter_dict = NcMLParser
```

The dictionary representation of the data model will however consist in a list of variable dictionaries, not a dictionary of a list of attributes. This is easily handled within the `intake_converter.Intake.catalog_entry` method by special casing the `variables` field. 


### Additional parsing

If raw attributes found in the NcML need some preprocessing before being entered in the catalog, a custom pydantic validator can be defined. See one example in `datamodels.cmip5.CMIP5`.



## Notes
CMIP defines a Data Reference Syntax (DRS) used to name files that is not identical to the CV, in part because the DRS syntax cannot include spaces, periods or parentheses. This is why there are often differences between the model or institute id found in the file name and that found in the global attributes metadata. Here, we use the CV rather than the DRS to define the catalog and validate the entries. The DRS is still a useful guide as to the minimal subset of attributes uniquely identify an item in a CMIP collection. 


### Simulations
CMIP, CORDEX vocabulary for file name
CMIP5 DRS: https://www.medcordex.eu/cmip5_data_reference_syntax.pdf
Should we split into project ? CMIP5 / CMIP6 / CORDEX
Should we split into regional vs global ?
https://github.com/WCRP-CMIP/CMIP6_CVs

## Bias adjusted projections
Home-made I suppose ?

### Atmospheric reanalyses
https://reanalyses.org/atmosphere/comparison-table

### Gridded observations
https://github.com/PCMDI/obs4MIPs-cmor-tables

### Station observations
No data on THREDDS

### Forecasts
Not a priority for now, only one dataset. 


See https://github.com/DACCS-Climate/roadmap/wiki/Catalog-design-and-architecture


Gab:
- grid resolution (degree)
- standard Not Applicable value
- institude_id (gcm/rcm)
- how-to ( for first member, highest-resolution)

# Open questions

- Variable coded with CMIP output variable name ? (only standard_name is in CF convention)
   * Can use synonym table from cf-index-meta
