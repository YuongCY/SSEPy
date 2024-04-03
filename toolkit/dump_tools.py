# -*- coding:utf-8 _*-
"""
LIB-SSE CODE
@author: Jeza Chen
@license: GPL-3.0 License
@file: dump_tools.py
@time: 2024/04/03
@contact: jeza@vip.qq.com
@site:
@software: PyCharm
@description: tools for dumping and loading objects
"""

__all__ = ['dump_object']

_TYPES_TO_USE_PICKLE = (dict, list, tuple, set, frozenset)
_DUMP_METHOD_NAME_OF_SSE_OBJECT = 'to_bytes'


def dump_object(obj) -> bytes:
    """ Dump an object to bytes
    :param obj: The object to be dumped
    :return: The dumped bytes
    """
    if isinstance(obj, _TYPES_TO_USE_PICKLE):
        import pickle
        return pickle.dumps(obj)
    elif hasattr(obj, _DUMP_METHOD_NAME_OF_SSE_OBJECT):
        return getattr(obj, _DUMP_METHOD_NAME_OF_SSE_OBJECT)()
    else:
        raise ValueError(f"Unsupported object type: {type(obj)}")


def load_object(obj_bytes: bytes, obj_type: type) -> object:
    """ Load an object from bytes
    :param obj_bytes: The bytes to be loaded
    :param obj_type: The type of the object
    :return: The loaded object
    """
    if obj_type in _TYPES_TO_USE_PICKLE:
        import pickle
        return pickle.loads(obj_bytes)
    else:
        return obj_type.from_bytes(obj_bytes)
