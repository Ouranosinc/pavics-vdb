from pystac import Extensions
from pystac.item import Item
from pystac.extensions.base import (ItemExtension, ExtensionDefinition, ExtendedObject)


CMIP5_EXTENSION_SCHEMA_URL = "TODO"

# Note : based on EOItemExt

class CMIP5ItemExt(ItemExtension):
    """CMIP5ItemExt is the extension of the Item in the cmip5 extension.

    Args:
        item (Item): The item to be extended.

    Attributes:
        item (Item): The Item that is being extended.

    Note:
        Using CMIP5ItemExt to directly wrap an item will add the 'cmip5' extension ID to
        the item's stac_extensions.
    """
    def __init__(self, item):
        if item.stac_extensions is None:
            item.stac_extensions = [CMIP5_EXTENSION_SCHEMA_URL]
        elif CMIP5_EXTENSION_SCHEMA_URL not in item.stac_extensions:
            item.stac_extensions.append(CMIP5_EXTENSION_SCHEMA_URL)

        self.item = item

    def apply(self, variable_id=None):
        """Applies label extension properties to the extended Item.

        Args:
            variable_id (string or None): The variable id.
        """
        self.variable_id = variable_id

    @property
    def variable_id(self):
        """Get or sets the variable id.

        Returns:
            string or None
        """
        return self.get_variable_id()

    @variable_id.setter
    def variable_id(self, v):
        self.set_variable_id(v)

    def get_variable_id(self, asset=None):
        """Gets an Item or an Asset variable_id.

        If an Asset is supplied and the Item property exists on the Asset,
        returns the Asset's value. Otherwise returns the Item's value

        Returns:
            string
        """
        if asset is None or 'cmip5:variable_id' not in asset.properties:
            return self.item.properties.get('cmip5:variable_id')
        else:
            return asset.properties.get('cmip5:variable_id')

    def set_variable_id(self, variable_id, asset=None):
        """Set an Item or an Asset variable_id.

        If an Asset is supplied, sets the property on the Asset.
        Otherwise sets the Item's value.
        """
        if asset is None:
            self.item.properties['cmip5:variable_id'] = variable_id
        else:
            asset.properties['cmip5:variable_id'] = variable_id

    def __repr__(self):
        return '<CMIP5ItemExt Item id={}>'.format(self.item.id)

    @classmethod
    def _object_links(cls):
        return []

    @classmethod
    def from_item(cls, item):
        return cls(item)