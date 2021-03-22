# STAC API TDS Climate Data Ingestion Pipeline PoC

This project shows a PoC of the different parts of the ingestion process.

## High level pipeline view

![pipeline view](assets/hl_arch.png "Pipeline")


## Notes

- Too avoid useless queries to Thredds, a file named `tds_cache.json` is created after each run of `tds_crawler.py`. Delete it to crawl again Thredds.


## Dependencies

Required
- STAC API running at `STAC_HOST` (see `.env.example`)
- THREDDS catalog at `THREDDS_CATALOG` (see `.env.example`)

Optional
- `stac-browser` git project cloned, to visualize static STAC catalog
    - In another terminal, current directory, to allow CORS, for development purpose:
        `mkdir output; cd output; http-server -p 8099 --cors`
 

## Demo

```
# Setup
[setup .env file]
pip install -r requirements.txt

# Crawl
python3 -m main_ingestion_svc                       # Populate STAC API with TDS data

# Query dynamic STAC API catalog
curl -s 0.0.0.0:8000/collections | jq -r '.[].id'                                 # Print all STAC collections that our local STAC API contains
curl -s 0.0.0.0:8000/collections/{collection_id}/items | jq -r '.features[].id'   # Print all STAC items that one local STAC API collection contains

# Visualize dynamic STAC catalog
cd [stac-browser repo]
CATALOG_URL=http://localhost:8000 npm start -- --open      # Restart on catalog update

# Visualize static STAC catalog
cd [stac-browser repo]
CATALOG_URL=http://localhost:8099/catalog.json npm start -- --open      # Restart on catalog update

# Clean
python3 -m stac_api_data_eraser                     # Remove all data from STAC API
```


## Other useful stuff

### Push static catalog to demo server

```
chmod +x ./Taskfile
./Taskfile push
```


### Schema validation

```
pip install stac-validator
stac_validator ./test_item.json --custom ./schemas/item.json 
```
