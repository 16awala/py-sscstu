import uuid
from .core.storage import Storage, StorageItem, StorageSearchIter
import os
import tempfile

# Copy from remote source to local disk, copy from local disk to remote destination, delete local disk copy
def _using_local_ehpemeral(
        source: Storage,
        objects: [list,StorageSearchIter],
        destination: [list,Storage],
        delete_source=False,
        destination_prefix = "",
        continue_on_error=False):
    with tempfile.TemporaryDirectory(suffix='sscstu' + str(uuid.uuid4())) as tmpdir:
        for o in objects:
            try:
                print(f"Transfer {o} from {source} to {destination}")
                tmppath = tmpdir + str(uuid.uuid4())
                source.get(o, tmppath)
                dests = destination if isinstance(destination, list) else [destination]

                for dest in dests:
                    if type(o) in dest.ItemTypes:
                        if not dest.put(o, tmppath, prefix=destination_prefix) and not continue_on_error:
                            raise Exception(f"Failed to put {o} from {tmppath}")
                    else:
                        # Use from_file on most preferred
                        if not destination.put(type(destination.ItemTypes[0]).from_file(tmppath, name=o.name), tmppath, prefix=destination_prefix) and not continue_on_error:
                            os.remove(tmppath)
                            raise Exception(f"Failed to put {o} from {tmppath}")
                os.remove(tmppath)
                if delete_source:
                    if not source.delete(o):
                        raise Exception(f"Failed to delete {o} from source")
            except Exception as e:
                if not continue_on_error:
                    raise e

def _using_mem_file():
    raise NotImplementedError()

def _using_async_fstreams():
    raise NotImplementedError()

def transfer(
        source: [Storage, str],
        destination: [Storage, list, str],
        source_basepath=None,
        destination_prefix="",
        delete_source=False,
        continue_on_error=False,
        buffer_type="auto",
        buffer_size=None):
    """

    Parameters
    ----------
    source - Storage object to GET objects from
    destination - Storage object to PUT objects to
    source_basepath - Source search string to be used to narrow selected objects for transfer
    destination_prefix - Prefix string to be added to all objects being PUT to destination. Implementation of this is up to the Storage class
    delete_source - On successful transfer from source to destination, should the file be deleted from source? Default: False
    continue_on_error - If an object fails to be transferred, should we move on to try other objects? If False, raise exception on first failure. Default: False
    buffer_type - Type of buffer to use for file transfer.
    buffer_size - Intended to limit the footprint of memory-based buffers

    Returns
    -------

    """
    if source_basepath is None:
        raise ValueError("Must specify a base search path for source Storage object")
    if buffer_type.lower() == 'auto':
        buffer_type = 'file'
    if buffer_type.lower() == 'file':
        return _using_local_ehpemeral(
            source,
            source.search(source_basepath),
            destination,
            destination_prefix=destination_prefix,
            delete_source=delete_source,
            continue_on_error=continue_on_error)
    elif buffer_type == 'memory':
        raise NotImplementedError()
    elif buffer_type == 'stream':
        raise NotImplementedError()
    else:
        raise ValueError(f"Unknown buffer type: {buffer_type}")