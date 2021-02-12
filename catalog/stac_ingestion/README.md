# STAC API TDS Climate Data Ingestion PoC

## High level pipeline view

![pipeline view](assets/hl_arch.png "Pipeline")

This current project shows a PoC of the different parts of the ingestion process.


## Requirements

- STAC API running at `STAC_HOST` (see `.env.example`)
- THREDDS catalog at `THREDDS_CATALOG` (see `.env.example`)


## Populate STAC API with TDS data

```
python3 -m main_ingestion_svc
```


## Remove all data from STAC API

```
python3 -m stac_api_data_eraser
```


## Validate schema

```
pip install stac-validator
stac_validator ./test_item.json --custom ./schemas/item.json 
```


## Run STAC-browser

```
# requires http-server, only to allow CORS, for dev
http-server -p 8000 --cors
CATALOG_URL=http://localhost:8000/catalog.json npm start -- --open
```