from .storage import StorageItem

# Generic classes covering some common cloud storage scenarios

class ObjectNotFoundError(Exception):
    pass


class ObjectPermissionError(Exception):
    pass


class UnsupportedItemTypeError(Exception):
    pass