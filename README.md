Super Simple Cloud Storage Transfer Utilities
========

This project is designed to be a *super* high level interface
for transferring files to and from various cloud storage solutions.

For example, copying a single folder from one S3 bucket to another:
```python3
from sscstu.storage.s3 import S3Storage, S3StorageItem
from sscstu.transfer import transfer

src = S3Storage(bucket_name='my-bucket')
dest = S3Storage(bucket_name='my-other-bucket')
transfer(src, dest, source_basepath="Foldername", destination_prefix="Foldername")

```

Installation
------------

    python3 -m pip install sscstu

Contribute
----------

- Issue Tracker: https://github.com/16awala/py-sscstu/issues
- Source Code: https://github.com/16awala/py-sscstu

License
-------

The project is licensed under the LGPLv3 license.