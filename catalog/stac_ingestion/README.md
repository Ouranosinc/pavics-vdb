# STAC API TDS Climate Data Ingestion PoC

## High level pipeline view

![pipeline view](assets/hl_arch.png "Pipeline")

This current project shows a PoC of the different parts of the ingestion process.


## Populate STAC API with TDS data

```
PYTHONPATH="${PYTHONPATH}:$PWD/.." python3 ./main_ingestion_svc.py
```


## Remove all data from STAC API

```
PYTHONPATH="${PYTHONPATH}:$PWD/.." python3 ./stac_api_data_eraser.py
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