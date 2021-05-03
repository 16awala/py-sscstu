from abc import ABC, abstractmethod, abstractproperty
import typing
import os
import io


# Signatures cause circular type dependencies
class Storage(ABC):
    pass

class StorageItem(ABC):
    pass

class StorageSearchIter(ABC):
    pass


class StorageItem(ABC):

    @staticmethod
    @abstractmethod
    def from_file(path, remote_path="", name=None):
        pass

    @staticmethod
    @abstractmethod
    def from_storage(o: Storage, path=""):
        pass


    @abstractmethod
    def __init__(self):
        super().__init__()

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def size(self):
        pass

    @property
    @abstractmethod
    def remote_path(self):
        pass

    def __str__(self):
        return self.name


class StorageSearchIter(ABC):
    """
    Represents a search of storage items. This iterator should not return the same individual item twice
    (that is, if there are two otherwise identical copies of an item on remote storage, each should be returned once)
    """

    @abstractmethod
    def __init__(self):
        super().__init__()

    def __iter__(self):
        return self

    @abstractmethod
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
    ItemTypes = [StorageItem]
    stream_support = False

    def __init__(self):
        super().__init__()

    @abstractmethod
    def search(self, basepath: str = ""):
        """
        Return an interator representing all objects within a specified search of the remote storage.
        Should generally return objects that are children of, or begin with basepath.
        :param basepath: Allow limiting to sub "directories" or other organizational schemas
        :return: StorageObjectIter representing all files within basepath
        :rtype: StorageObjectIter
        """

    @abstractmethod
    def put(self, o: StorageItem, filepath: str, prefix: str = ""):
        """
        Upload file from filepath to remote storage
        :param o: Remote storage representation
        :param filepath: local file location
        :param prefix: Prefix to append to remote file location
        :return: Success?
        :rtype: bool
        """
        pass

    def put_stream(self, o: StorageItem, file: io.IOBase, prefix: str = ""):
        """
        Download file from remote storage to local IO Stream
        :param o: Remote storage object representation
        :param file: IO object to write a binary stream to
        :param prefix: Prefix to append to remote file location
        :return: Success?
        :rtype: bool
        """
        raise NotImplemented

    @abstractmethod
    def get(self, o: StorageItem, filepath: str = ""):
        """
        Download file from remote storage to filepath
        :param o: Remote storage object representation
        :param filepath: optional information to associate with the remote object, eg s3 tags, expiry, or storage settings
        :return: Success?
        :rtype: bool
        """
        pass

    @abstractmethod
    def fetch(self, o: StorageItem):
        """
        Fetch details about a remote object and return a object containing that information
        The intent is for a user to create a base StorageItem object containing enough information to locate the file
            in question (say, a remote path for example), and then use this method to fetch all actual metadata about
            the object that was not known prior. The method should not modify o, but should create a new StorageItem
        :param o: Remote storage object representation
        :return: o?
        :rtype: StorageObject
        """
        pass

    def get_stream(self, o: StorageItem, file: io.IOBase):
        """
        Download file from remote storage to local IO Stream
        :param o: Remote storage object representation
        :param file: IO object to write a binary stream to
        :return: Success?
        :rtype: bool
        """
        raise NotImplemented

    @abstractmethod
    def delete(self, o: StorageItem):
        """
        Delete file from remote storage
        :param o: Remote storage representation
        :return: Success?
        :rtype: bool
        """
        pass