from ..core.storage import StorageItem, StorageSearchIter, Storage
from ..core.exceptions import ObjectNotFoundError, UnsupportedItemTypeError
import boto3
import botocore
import os
import io
import builtins
import pathlib


class S3StorageItem(StorageItem):
    pass

class S3StorageSearchIter(StorageSearchIter):
    pass

class S3Storage(Storage):
    pass

class S3StorageItem(StorageItem):
    DeleteMarker = None
    AcceptRanges = None
    Expiration = None
    Restore = None
    ArchiveStatus = None
    LastModified = None
    ContentLength = None
    ETag = None
    MissingMeta = None
    VersionId = None
    CacheControl = None
    ContentDisposition = None
    ContentEncoding = None
    ContentLanguage = None
    ContentType = None
    Expires = None
    WebsiteRedirectLocation = None
    ServerSideEncryption = None
    Metadata = None
    SSECustomerAlgorithm = None
    SSECustomerKeyMD5 = None
    SSEKMSKeyId = None
    BucketKeyEnabled = None
    StorageClass = None
    RequestCharged = None
    ReplicationStatus = None
    PartsCount = None
    ObjectLockMode = None
    ObjectLockRetainUntilDate = None
    ObjectLockLegalHoldStatus = None
    Key = None

    @staticmethod
    def from_file(path, remote_path="", name=None):
        if name is None:
            name = pathlib.PurePath(path).name
        return S3StorageItem(Key=name)

    @staticmethod
    def from_storage(s3: S3Storage, key: str):
        return s3.fetch(S3StorageItem(Key=key))

    @staticmethod
    def from_dict(d: dict, Key=None):
        if Key is None:
            if 'Key' not in d:
                raise ValueError("Must specify a key")
            else:
                Key = d['Key']
        i = S3StorageItem(Key=Key)
        for key in d:
            if key in i.__dict__:
                i.__dict__[key] = d[key]
        i.Key = Key # Enforces manually specified Key if there was also a Key in the dict
        return i

    def __init__(self, Key=None):
        self.Key = Key
        if self.Key is None:
            raise ValueError("Must specify a key")
        super().__init__()

    @property
    def name(self):
        return self.Key.split('/')[-1]

    @property
    def size(self):
        return self.ContentLength

    @property
    def remote_path(self):
        return self.Key

    def __str__(self):
        return self.Key


class S3StorageSearchIter(StorageSearchIter):
    def __init__(self,
                 client: [boto3.Session, botocore.client] = None,
                 bucket: str = None, basepath: str = "",
                 pagesize: int = 100):
        self._client = client
        self._bucket = bucket
        self._basepath = basepath
        self._pagesize = pagesize

        self._response = self._client.list_objects_v2(
            Bucket=self._bucket,
            MaxKeys=self._pagesize,
            Prefix=self._basepath
        )
        self._index = 0
        super().__init__()

    def __iter__(self):
        return self

    def _fetchNewS3Page(self):
        if not self._response['IsTruncated']:
            return False
        self._response = self._client.list_objects_v2(
            Bucket=self._bucket,
            MaxKeys=self._pagesize,
            Prefix=self._basepath,
            ContinuationToken=self._response['NextContinuationToken']
        )
        return True

    def __str__(self):
        return f"s3://{self._bucket}/{self._basepath}*"

    def __next__(self):
        index = self._index
        if index >= self._pagesize:
            if not self._fetchNewS3Page():
                raise StopIteration
            index = 0
        content = self._response['Contents']
        if index >= len(content):
            raise StopIteration
        ret = content[index]
        self._index = index + 1
        return S3StorageItem.from_dict(d=ret)


class S3Storage(Storage):
    _client = None
    _bucket = None
    ItemTypes = [S3StorageItem]
    stream_support = False

    def __init__(self,
                 session: [boto3.Session, botocore.client] = None,
                 bucket_name: str = None,
                 aws_access_key_id = None,
                 aws_secret_access_key = None,
                 endpoint_url = None):
        if bucket_name is None:
            raise ValueError("Must specify a bucket")
        if session is None:
            if (aws_access_key_id is None) and (aws_secret_access_key is None):
                self._client = boto3.Session().client('s3')
            if (aws_access_key_id is None) != (aws_secret_access_key is None):
                raise ValueError("Incomplete credentials")
            else:
                self._client = boto3.Session().client(
                    service_name='s3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    endpoint_url=endpoint_url
                )
        elif isinstance(session, boto3.Session):
            self._client = session.client(
                service_name='s3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                endpoint_url=endpoint_url
            )
        else: # we're gonna assume this is a client now, but we can't check easily because boto3 is built really weirdly
            self._client = session
        self._bucket = bucket_name
        super().__init__()

    def search(self, basepath: str = "", pagesize: int = 100):
        return S3StorageSearchIter(self._client, self._bucket, basepath=basepath, pagesize=pagesize)

    def put(self, o: S3StorageItem, filepath: str, prefix: str = ""):
        if type(o) not in self.ItemTypes:
            raise UnsupportedItemTypeError

        self._client.upload_file(
            filepath,
            self._bucket,
            prefix + o.Key
        )

        return True

    def put_stream(self, o: S3StorageItem, file: io.IOBase, callback = None, config = None, prefix: str = ""):
        """
        Download file from remote storage to local IO Stream
        :param o: Remote storage object representation
        :param file: IO object to write a binary stream to
        :param callback: Callback for S3 download accepting number of bytes
        :param config: Boto3 Transfer Config for controlling bytes
        :param prefix: Prefix to append to key
        :return: Success?
        :rtype: bool
        """
        self._client.upload_fileobj(
            Fileobj=file,
            Bucket=self._bucket,
            Key=prefix + o.Key,
            Callback=callback,
            Config=config
        )
        raise NotImplemented

    def get(self, o: S3StorageItem, filepath=""):
        if type(o) not in self.ItemTypes:
            raise ValueError(f"Unsupported Item type {str(type(o))}")
        key = o.Key
        tmp = filepath.rsplit('/', 1)
        if filepath == "":
            filepath = tmp[1]
        if tmp != "":
            os.makedirs(tmp[0], exist_ok=True)
        self._client.download_file(self._bucket, key, filepath)
        return True

    def fetch(self, o: S3StorageItem):
        """
        Fetch object info
        :param o: Remote storage object representation
        :return: Duplicate o containing additional metadata
        :rtype: S3StorageItem
        """
        try:
            ret = self._client.head_object(
                Bucket=self._bucket,
                Key=o.Key,
            )
            return S3StorageItem.from_dict(ret, Key=o.Key)
        except self._client.exceptions.NoSuchKey as e:
            raise ObjectNotFoundError(f"Key f{o.Key} does not exist")

    def get_stream(self, o: S3StorageItem, file: io.IOBase, callback = None, config = None):
        """
        Download file from remote storage to local IO Stream
        :param o: Remote storage object representation
        :param file: IO object to write a binary stream to
        :param callback: Callback for S3 download accepting number of bytes
        :param config: Boto3 Transfer Config for controlling bytes
        :return: Success?
        :rtype: bool
        """
        self._client.download_fileobj(
            Fileobj=file,
            Bucket=self._bucket,
            Key=o.Key,
            Callback=callback,
            Config=config
        )
        return True

    def delete(self, o: StorageItem):
        if type(o) not in self.ItemTypes:
            raise ValueError(f"Unsupported Item type {str(type(o))}")
        key = o.Key

        self._client.delete_object(
            Bucket=self._bucket,
            Key=key
        )
        return True

    def __str__(self):
        return f"s3://{self._bucket}"

