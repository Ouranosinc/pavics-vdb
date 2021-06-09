from stac_pydantic import Item, ItemProperties, Extensions
from pydantic import Field

class CustomProperties(Extensions.view, ItemProperties):
    row: int = Field(alias="landsat:row")

class CustomItem(Item):
    properties: CustomProperties # Override properties model

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

item = CustomItem(**stac_item)
assert item.properties.off_nadir == 3.78