from abc import ABC, abstractmethod
import typing

class StorageItem(ABC):
    """
    Represents an object in remote storage.
    Construction and internal mechanisms are relatively loose. The intention is for the StorageItem object to contain
    any information needed by a Storage object to identify the item in remote storage.
    This class should not be generic. It should contain as many fields as are relevant for the associated remote storage
    ...
    Attributes
    ----------
    name : str
        Generic file name to be used by external code as a common identifier. Generally should contain just a file
        name, not any path/filestructure information
    """
    name = None

    def __init__(self):
        super().__init__()

    def __str__(self):
        return self.name


class StorageSearchIter(ABC):
    """
    Represents a search of storage items. This iterator should not return the same individual item twice
    (that is, if there are two otherwise identical copies of an item on remote storage, each should be returned once)
    """
    def __init__(self):
        super().__init__()

    def __iter__(self):
        return self


    def __next__(self):
        """
        Iterate through whatever remote file tree is involved and return individual file objects
        :return: StorageObject representing a single file object
        :rtype: StorageObject
        """
        pass


class Storage(ABC):
    """
    Represents a "connection" to remote storage. All methods may implement additional OPTIONAL parameters, but generally
    should store within itself any other information needed to execute searches, puts, gets, and deletes.
    The exception being the __init__ method, which may involve more strict and platform-specific requirements, such as
    credentials.
    ...
    Attributes
    ----------
    ItemTypes
    """
    ItemTypes = [type(StorageItem)]

    def __init__(self):
        super().__init__()

    def search(self, basepath: str = ""):
        """
        Return an interator representing all objects within a specified search of the remote storage.
        Should generally return objects that are children of, or begin with basepath.
        :param basepath: Allow limiting to sub "directories" or other organizational schemas
        :return: StorageObjectIter representing all files within basepath
        :rtype: StorageObjectIter
        """

    def put(self, o: StorageItem, filepath: str):
        """
        Upload file from filepath to remote storage
        :param o: Remote storage representation
        :param filepath: local file location
        :return: Success?
        :rtype: bool
        """
        pass

    def get(self, o: StorageItem, filepath: str = ""):
        """
        Download file from remote storage to filepath
        :param o: Remote storage object representation
        :param filepath: optional information to associate with the remote object, eg s3 tags, expiry, or storage settings
        :return: Success?
        :rtype: bool
        """
        pass

    def delete(self, o: StorageItem):
        """
        Delete file from remote storage
        :param o: Remote storage representation
        :return: Success?
        :rtype: bool
        """
        pass
