from stac_pydantic import Extensions, Item
from pydantic import BaseModel, Field


# 1. Create a model for the extension
class LandsatExtension(BaseModel):
    row: int = Field(alias="landsat:row2")
    column: int = Field(None, alias="landsat:column")

    # Setup extension namespace in model config
    class Config:
        allow_population_by_fieldname = True
        alias_generator = lambda field_name: f"landsat:{field_name}"


# 2. Register the extension
Extensions.register("landsat", LandsatExtension) #, alias="https://example.com/stac/landsat-extension/1.0/schema.json")

# 3. Use model as normal
stac_item = {
    "id": "sample_id",
    "type": "Feature",
    "stac_extensions": [
        "landsat",
        "view"
    ],
    "bbox": [ 49.16354, 72.27502, 51.36812, 75.67662 ],
    "geometry": { "type": "Polygon", "coordinates": [ [ [ 51.33855, 72.27502 ], [ 51.36812, 75.70821 ], [ 49.19092, 75.67662 ], [ 49.16354, 72.3964 ], [ 51.33855, 72.27502 ] ] ] },
    "properties": {
        "datetime": "2020-03-09T14:53:23.262208+00:00",
        "view:off_nadir": 3.78,
        "landsat:row": "230",
        "landsat:column": 178
    },
    "links": [],
    "assets": {},
}

item = Item(**stac_item)
# item_dict = item.to_dict()
# # assert item_dict['properties']['landsat:row'] == "230"
# assert item_dict['properties']['landsat:column'] == 178
