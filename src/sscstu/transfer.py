import uuid
from .core.storage import Storage, StorageItem, StorageSearchIter
import os
import tempfile

# Copy from remote source to local disk, copy from local disk to remote destination, delete local disk copy
def _using_local_ehpemeral(source: Storage, objects: [list,StorageSearchIter], destination: Storage, delete_source=False, destination_prefix = ""):
    with tempfile.TemporaryDirectory(suffix='sscstu' + str(uuid.uuid4())) as tmpdir:
        for o in objects:
            print(f"Transfer {o} from {source} to {destination}")
            tmppath = tmpdir + str(uuid.uuid4())
            source.get(o, tmppath)
            if type(o) in destination.ItemTypes:
                if not destination.put(o, tmppath, prefix=destination_prefix):
                    raise Exception(f"Failed to put {o} from {tmppath}")
            else:
                # Use from_file on most preferred
                if not destination.put(type(destination.ItemTypes[0]).from_file(tmppath, name=o.name), tmppath, prefix=destination_prefix):
                    os.remove(tmppath)
                    raise Exception(f"Failed to put {o} from {tmppath}")
            os.remove(tmppath)
            if delete_source:
                if not source.delete(o):
                    raise Exception(f"Failed to delete {o} from source")

def _using_mem_ephemeral():
    raise NotImplementedError()

def _using_async_fstreams():
    raise NotImplementedError()

def transfer(source: [Storage, str], destination: [Storage, str], source_basepath=None, destination_prefix=""):
    if source_basepath is None:
        raise ValueError("Must specify a base search path for source Storage object")
    return _using_local_ehpemeral(source, source.search(source_basepath), destination, destination_prefix=destination_prefix)