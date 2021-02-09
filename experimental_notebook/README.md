
## Populate STAC API with TDS data

```
PYTHONPATH="${PYTHONPATH}:$PWD" python3 ./experimental_notebook/main_ingestion_svc.py
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