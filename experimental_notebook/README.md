
## Populate STAC API with TDS data

```
PYTHONPATH="${PYTHONPATH}:$PWD/.." python3 ./main_ingestion_svc.py
```


## Remove all data from STAC API

```
python3 ./stac_api_data_eraser.py
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