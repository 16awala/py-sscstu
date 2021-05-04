from ..core.storage import Storage, StorageSearchIter, StorageItem
from pathlib import Path
import shutil
from shutil import SameFileError
import os
import io

class LocalStorage(Storage):
    pass

class LocalStorageItem(StorageItem):
    pass

class LocalStorageSearchIter(StorageSearchIter):
    pass


class LocalStorageItem(StorageItem):
    path = None
    @staticmethod
    def from_file(path, remote_path="", name=None):
        if remote_path == "":
            remote_path = path
        if name is None:
            name = remote_path
        return LocalStorageItem(name)


    @staticmethod
    def from_storage(o: Storage, path=""):
        pass

    def __init__(self, path=""):
        self.path = path
        super().__init__()

    @property
    def name(self):
        return Path(self.path).name

    @property
    def size(self):
        return os.stat(self.path).st_size

    @property
    def remote_path(self):
        return self.path

    def __str__(self):
        return self.name


class LocalStorageSearchIter(LocalStorage):
    """
    Represents a search of storage items. This iterator should not return the same individual item twice
    (that is, if there are two otherwise identical copies of an item on remote storage, each should be returned once)
    """
    ItemTypes = [LocalStorageItem]

    def __init__(self, basepath: str):
        l = os.listdir(basepath)
        self._basepath = basepath
        self._contents = l
        self._index = 0
        super().__init__()

    def __iter__(self):
        return self

    def __next__(self):
        """ d
        Iterate through whatever remote file tree is involved and return individual file objects
        :return: StorageObject representing a single file object
        :rtype: StorageObject
        """
        i = self._index
        c = self._contents
        if i < len(c):
            self._index = i + 1
            return LocalStorageItem(c[i])
        else:
            raise StopIteration

    def __str__(self):
        return self._basepath




class LocalStorage(Storage):
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
        self.ItemTypes = [StorageItem]


    def search(self, basepath: str = ""):
        """
        Return an interator representing all objects within a specified search of the remote storage.
        Should generally return objects that are children of, or begin with basepath.
        :param basepath: Allow limiting to sub "directories" or other organizational schemas
        :return: StorageObjectIter representing all files within basepath
        :rtype: StorageObjectIter
        """
        return LocalStorageSearchIter(basepath)


    def put(self, o: LocalStorageItem, filepath: str, prefix: str = ""):
        """
        Upload file from filepath to remote storage
        :param o: Remote storage representation
        :param filepath: local file location
        :return: Success?
        :rtype: bool
        """
        try:
            os.makedirs(Path(prefix + o.path).parent, exist_ok=True)
            shutil.copy(filepath, prefix + o.path)
        except SameFileError:
            pass
        return True

    def put_stream(self, o: StorageItem, file: io.IOBase, prefix: str = ""):
        """
        Download file from remote storage to local IO Stream
        :param o: Remote storage object representation
        :param file: IO object to write a binary stream to
        :return: Success?
        :rtype: bool
        """
        raise NotImplemented


    def get(self, o: LocalStorageItem, filepath: str = ""):
        """
        Download file from remote storage to filepath
        :param o: Remote storage object representation
        :param filepath: optional information to associate with the remote object, eg s3 tags, expiry, or storage settings
        :return: Success?
        :rtype: bool
        """
        try:
            os.makedirs(Path(filepath).parent, exist_ok=True)
            shutil.copy(o.path, filepath)
        except SameFileError:
            pass
        return True


    def fetch(self, o: LocalStorageItem):
        """
        Fetch details about a remote object and return a object containing that information
        The intent is for a user to create a base StorageItem object containing enough information to locate the file
            in question (say, a remote path for example), and then use this method to fetch all actual metadata about
            the object that was not known prior. The method should not modify o, but should create a new StorageItem
        :param o: Remote storage object representation
        :return: o?
        :rtype: StorageObject
        """
        return LocalStorageItem.from_file(o.path)

    def get_stream(self, o: StorageItem, file: io.IOBase):
        """
        Download file from remote storage to local IO Stream
        :param o: Remote storage object representation
        :param file: IO object to write a binary stream to
        :return: Success?
        :rtype: bool
        """
        raise NotImplemented


    def delete(self, o: LocalStorageItem):
        """
        Delete file from remote storage
        :param o: Remote storage representation
        :return: Success?
        :rtype: bool
        """
        os.remove(o.path)