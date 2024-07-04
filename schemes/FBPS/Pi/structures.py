# -*- coding:utf-8 _*-
""" 
FBPS CODE
@author: Yuhong Zhang
@license: GPL-3.0 License
@file: structure.py
@time: 2024/07/06
@contact: yuongys@163.com
@site:  
@software: PyCharm 
@description:  
"""
import pickle

from schemes.FBPS.Pi.config import PiConfig, PI_HEADER
from schemes.interface.structures import SSEKey, SSEEncryptedDatabase, SSEToken, SSEResult


class PiBasKey(SSEKey):
    __slots__ = ["K"]

    def __init__(self, K: bytes, config: PiConfig = None):
        super(PiBasKey, self).__init__(config)
        self.K = K

    def serialize(self) -> bytes:
        return self.K

    @classmethod
    def deserialize(cls, xbytes: bytes, config: PiConfig):
        if len(xbytes) != config.param_lambda:
            raise ValueError("The length of xbytes must be the same as the length of the parameter param_lambda.")

        return cls(xbytes)

    def __eq__(self, other):
        if not isinstance(other, PiBasKey):
            return False
        return self.K == other.K


class PiBasEncryptedDatabase(SSEEncryptedDatabase):
    __slots__ = ["D"]  # dict D

    def __init__(self, D: dict, config: PiConfig = None):
        super(PiBasEncryptedDatabase, self).__init__(config)
        self.D = D

    @classmethod
    def build_from_list(cls, kv_pairs: list, config: PiConfig = None):
        kv_pairs.sort(key=lambda pair: pair[0])
        D = {key: value for key, value in kv_pairs}
        return cls(D, config)

    def serialize(self) -> bytes:
        data = PI_HEADER + pickle.dumps(self.D)
        return data

    @classmethod
    def deserialize(cls, xbytes: bytes, config: PiConfig = None):
        if xbytes[:len(PI_HEADER)] != PI_HEADER:
            raise ValueError("Parse header error.")

        data_bytes = xbytes[len(PI_HEADER):]
        D = pickle.loads(data_bytes)
        return cls(D)

    def __eq__(self, other):
        if not isinstance(other, PiBasEncryptedDatabase):
            return False
        return self.D == other.D


class PiBasToken(SSEToken):
    __slots__ = ["K1", "K2"]  # K1, K2

    def __init__(self, K1: bytes, K2: bytes, config: PiConfig = None):
        super(PiBasToken, self).__init__(config)
        self.K1 = K1
        self.K2 = K2

    def serialize(self) -> bytes:
        return self.K1 + self.K2

    @classmethod
    def deserialize(cls, xbytes: bytes, config: PiConfig = None):
        if len(xbytes) != 2 * config.param_lambda:
            raise ValueError("The length of xbytes must be 2 times the length of the parameter param_lambda.")

        K1, K2 = xbytes[:config.param_lambda], xbytes[config.param_lambda:]

        return cls(K1, K2, config)

    def __eq__(self, other):
        if not isinstance(other, PiBasToken):
            return False
        return self.K1 == other.K1 and self.K2 == other.K2


class PiBasResult(SSEResult):
    __slots__ = ["result"]

    def __init__(self, result: list, config: PiConfig = None):
        super(PiBasResult, self).__init__(config)
        self.result = result

    def serialize(self) -> bytes:
        return pickle.dumps(self.result)

    @classmethod
    def deserialize(cls, xbytes: bytes, config: PiConfig = None):
        result = pickle.loads(xbytes)
        if not isinstance(result, list):
            return ValueError("The data contained in xbytes is not a list.")

        return cls(result, config)

    def __str__(self):
        return self.result.__str__()

    def __eq__(self, other):
        if not isinstance(other, PiBasResult):
            return False
        return self.result == other.result

    def get_result_list(self) -> list:
        return self.result
