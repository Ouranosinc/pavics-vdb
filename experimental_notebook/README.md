
## Populate STAC API with data

```
python3 ./stac-ingest.py
```


## Validate schema

```
pip install stac-validator
stac_validator ./test_item.json --custom ./schemas/item.json 
```