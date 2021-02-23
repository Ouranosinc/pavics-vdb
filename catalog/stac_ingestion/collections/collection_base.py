from abc import (ABC, abstractmethod)


class CollectionBase(ABC):
    """
    Abstract class defining dataset metadata schema processing.
    """
    def __init__(self):
        pass

    @abstractmethod
    def add_metadata(self, ds):
        """

        """
        pass

    @abstractmethod
    def get_stac_collection(self, item):
        """

        """
        pass

    @abstractmethod
    def get_stac_collection_item(self, collection_items, collection_name):
        """

        """
        pass