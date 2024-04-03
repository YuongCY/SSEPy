# -*- coding:utf-8 _*-
""" 
LIB-SSE CODE
@author: Jeza Chen
@license: GPL-3.0 License
@file: persistent_array.py
@time: 2022/05/08
@contact: jeza@vip.qq.com
@site:  
@software: PyCharm 
@description: 
"""
import collections.abc
import os.path
import shutil
import threading
import typing

from data_persistence.interfaces import PersistentBytesDict
import data_persistence.bytes_shelf
import pickle

__all__ = ["PickledDict", "DBMDict"]

from toolkit.temp_tools import make_temp_path


class _ClosedDict(collections.abc.MutableMapping):
    """Marker for a closed dict.  Access attempts raise a ValueError."""

    def closed(self, *args):
        raise ValueError('invalid operation on closed dict')

    __iter__ = __len__ = __getitem__ = __setitem__ = __delitem__ = __eq__ = keys = closed

    def __repr__(self):
        return '<Closed Dictionary>'


class PickledDict(PersistentBytesDict):
    """ A naive persistent dictionary, based on dict and pickle lib
    """

    @classmethod
    def open(cls, local_path: str = '', create_only: bool = False) -> 'PickledDict':
        return cls(local_path, create_only)

    @classmethod
    def create(cls, local_path: str = '') -> 'PickledDict':
        return cls(local_path, True)

    def __init__(self, file_path: str = '', is_new_file: bool = False):
        if not file_path:
            # create temporary file
            if not is_new_file:
                raise ValueError("Please specify the file path when opening an existing file.")
            file_path = make_temp_path()
            self.__is_temp = True
        else:
            self.__is_temp = False

        self.__file_path = file_path
        self.__data = {}
        self.__file = None
        if not is_new_file:  # read a created dict
            try:
                self.__file = open(file_path, "rb+")
                self.__data = pickle.load(self.__file)
                if not isinstance(self.__data, typing.Dict):
                    raise TypeError(f"The data of argument file_path {file_path} is not an instance of dict")
            except FileNotFoundError:
                raise FileNotFoundError(f"The dict corresponding to the local path {file_path} not exists.")
            except (TypeError, pickle.UnpicklingError):
                self.__file.close()
                raise

        else:  # create a new dict
            if os.path.exists(file_path):
                raise FileExistsError(f"The file {file_path} exists.")
            self.__file = open(file_path, "wb+")

    def sync(self) -> None:
        self.__file.truncate(0)
        self.__file.seek(0)
        pickle.dump(self.__data, self.__file)
        self.__file.flush()

    def close(self) -> None:
        try:
            if not self.__file.closed:
                self.sync()
                self.__file.close()
        except AttributeError:  # self.__file may be None
            pass
        finally:
            try:
                self.__data = _ClosedDict()
            except:
                self.__data = None

    def release(self) -> None:
        self.close()
        if os.path.exists(self.__file_path):  # may be released multiple times
            os.unlink(self.__file_path)

    def __iter__(self):
        return iter(self.__data)

    def __len__(self):
        return len(self.__data)

    def get(self, key: bytes, default=None):
        return self.__data.get(key, default)

    def __contains__(self, key: bytes):
        return key in self.__data

    def __getitem__(self, key: bytes):
        return self.__data[key]

    def __setitem__(self, key: bytes, value: bytes):
        # check if the value is a byte-string
        if not isinstance(value, typing.ByteString):
            raise TypeError(
                "The content should be a byte string."
            )

        self.__data[key] = value

    def __delitem__(self, key: bytes):
        del self.__data[key]

    def __eq__(self, other: 'PickledDict'):
        if not isinstance(other, PickledDict):
            return False
        return self.__data == other.__data

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()
        if self.__is_temp:
            self.release()

    def clear(self):
        self.__data = {}

    @property
    def dict_local_path(self):
        return self.__file_path

    @classmethod
    def from_dict(cls, dict_: dict, dict_path: str = '') -> 'PickledDict':
        pickled_dict = cls(dict_path, True)
        pickled_dict.__data = dict(dict_)  # Be Careful, Copy!
        pickled_dict.sync()
        return pickled_dict

    def to_bytes(self):
        return pickle.dumps(self.__data)

    @classmethod
    def from_bytes(cls, bytes_: bytes, dict_path: str = '') -> 'PickledDict':
        pickled_dict = cls(dict_path, True)
        pickled_dict.__data = pickle.loads(bytes_)
        pickled_dict.sync()
        return pickled_dict


class DBMDict(PersistentBytesDict):
    """ A simple persistent dictionary, based on shelve.BytesShelf
    """

    """ Threading Lock
    Dbm does not allow concurrent opening of database files.
    """
    __thread_lock_map = collections.defaultdict(lambda: threading.Lock())

    """ Real Database Filename for underlying DBM
    """
    _real_db_filename = 'db'

    @classmethod
    def open(cls, local_path: str, create_only: bool = False) -> 'DBMDict':
        return cls(local_path, create_only)

    @classmethod
    def create(cls, local_path: str) -> 'DBMDict':
        return cls(local_path, True)

    def __init__(self, file_path: str, is_new_file: bool = False):
        if not file_path:
            # create temporary file
            if not is_new_file:
                raise ValueError("Please specify the file path when opening an existing file.")
            file_path = make_temp_path()
            self.__is_temp = True
        else:
            self.__is_temp = False

        self.__file_path = file_path
        self.__closed = False
        self.__shelf = None

        # check validity only
        if not is_new_file:  # read
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"The dict corresponding to the local path {file_path} not exists.")
        else:  # create
            if os.path.exists(file_path):
                raise FileExistsError(f"The file {file_path} exists.")
        self.__thread_lock_map[file_path].acquire()
        os.makedirs(self.__file_path, exist_ok=True)
        self.__real_path = os.path.join(os.path.abspath(file_path), type(self)._real_db_filename)

        self.__shelf = data_persistence.bytes_shelf.open(self.__real_path, writeback=True)

    def sync(self) -> None:
        self.__shelf.sync()

    def close(self) -> None:
        if self.__closed:  # Already Closed
            return
        try:
            self.__shelf.close()
        except AttributeError:  # __shelf may be None, because it may be released when __init__ method is not completed
            pass
        finally:
            try:
                self.__closed = True
                self.__thread_lock_map[self.__file_path].release()
                self.__shelf = _ClosedDict()
            except RuntimeError as e:
                if len(e.args) != 1 or e.args[0] != "release unlocked lock":
                    raise
            except:
                self.__shelf = None

    def release(self) -> None:
        self.close()
        if os.path.exists(self.__file_path):  # may be released multiple times
            # os.unlink(self.__file_path)
            shutil.rmtree(self.__file_path)

    def __iter__(self):
        return iter(self.__shelf)

    def __len__(self):
        return len(self.__shelf)

    def get(self, key: bytes, default=None):
        return self.__shelf.get(key, default=default)

    def __contains__(self, key: bytes):
        return key in self.__shelf

    def __getitem__(self, key: bytes):
        return self.__shelf[key]

    def __setitem__(self, key: bytes, value):
        # check if the value is a byte-string
        if not isinstance(value, typing.ByteString):
            raise TypeError(
                "The content should be a byte string."
            )

        self.__shelf[key] = value

    def __delitem__(self, key: bytes):
        del self.__shelf[key]

    def __eq__(self, other):
        if not isinstance(other, DBMDict):
            return False
        return self.__shelf == other.__shelf

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()
        if self.__is_temp:
            self.release()

    def clear(self):
        self.__shelf.clear()

    @property
    def dict_local_path(self):
        return self.__file_path

    @classmethod
    def from_dict(cls, dict_: dict, dict_path: str = '') -> 'DBMDict':
        pickled_dict = cls(dict_path, True)
        pickled_dict.__shelf.update(dict_)
        pickled_dict.sync()
        return pickled_dict

    @classmethod
    def get_real_db_filename(cls):
        return cls._real_db_filename

    def to_bytes(self) -> bytes:
        dict_ = dict(self.__shelf)
        return pickle.dumps(dict_)

    @classmethod
    def from_bytes(cls, bytes_: bytes, dict_path: str) -> 'DBMDict':
        pickled_dict = cls(dict_path, True)
        pickled_dict.__shelf.update(pickle.loads(bytes_))
        pickled_dict.sync()
        return pickled_dict
