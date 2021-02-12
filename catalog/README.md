# PAVICS Catalog

We want catalogs to access datasets used in the production of climate services. We have different kinds of datasets, climate simulations, reanalyses, gridded and at-site observations, bias-corrected projections, etc. These datasets are typically described by different attributes. It thus makes sense to create independent catalog entries, and leave it to users to select the right catalog. 

## Attributes and controlled vocabulary
In a catalog, each dataset is described by a set of attributes, forming a catalog entry. Based on consultations, it appears useful to include in those entries descriptive fields that go beyond simple dataset identification. That is, entries such as model resolution can be useful to filter datasets. 

To ensure catalog clarity, it is important that attributes are described as much as possible by a well-defined controlled vocabulary (CV). A CV describes the attributes and the possible values it can take. Validating values ensures that search queries are not sabotaged by slight naming differences.   

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







