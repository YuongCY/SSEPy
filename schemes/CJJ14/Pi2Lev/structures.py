# -*- coding:utf-8 _*-
""" 
LIB-SSE CODE
@author: Jeza Chen
@license: GPL-3.0 License 
@file: structures.py 
@time: 2022/03/11
@contact: jeza@vip.qq.com
@site:  
@software: PyCharm 
@description: 
"""
import pickle

from schemes.CJJ14.Pi2Lev.config import Pi2LevConfig, PI_2LEV_HEADER
from schemes.interface.structures import SSEKey, SSEEncryptedDatabase, SSEToken, SSEResult


class Pi2LevKey(SSEKey):
    __slots__ = ["K"]

    def __init__(self, K: bytes, config: Pi2LevConfig = None):
        super(Pi2LevKey, self).__init__(config)
        self.K = K

    def serialize(self) -> bytes:
        return self.K

    @classmethod
    def deserialize(cls, xbytes: bytes, config: Pi2LevConfig):
        if len(xbytes) != config.param_lambda:
            raise ValueError(
                "The length of xbytes must be the same as the length of the parameter param_k."
            )

        return cls(xbytes)

    def __eq__(self, other):
        if not isinstance(other, Pi2LevKey):
            return False
        return self.K == other.K


class Pi2LevEncryptedDatabase(SSEEncryptedDatabase):
    __slots__ = ["D", "A"]  # dict D, array A

    def __init__(self, D: dict, A: list, config: Pi2LevConfig = None):
        super(Pi2LevEncryptedDatabase, self).__init__(config)
        self.D = D
        self.A = A

    @staticmethod
    def create_dictionary_from_list(kv_pairs: list) -> dict:
        kv_pairs.sort(key=lambda pair: pair[0])
        D = {key: value for key, value in kv_pairs}
        return D

    def serialize(self) -> bytes:
        data = PI_2LEV_HEADER + pickle.dumps((self.D, self.A))
        return data

    @classmethod
    def deserialize(cls, xbytes: bytes, config: Pi2LevConfig = None):
        if xbytes[:len(PI_2LEV_HEADER)] != PI_2LEV_HEADER:
            raise ValueError("Parse header error.")

        data_bytes = xbytes[len(PI_2LEV_HEADER):]
        D, A = pickle.loads(data_bytes)
        return cls(D, A)

    def __eq__(self, other):
        if not isinstance(other, Pi2LevEncryptedDatabase):
            return False
        return self.D == other.D and self.A == other.A


class Pi2LevToken(SSEToken):
    __slots__ = ["K1", "K2"]  # K1, K2

    def __init__(self, K1: bytes, K2: bytes, config: Pi2LevConfig = None):
        super(Pi2LevToken, self).__init__(config)
        self.K1 = K1
        self.K2 = K2

    def serialize(self) -> bytes:
        return self.K1 + self.K2

    @classmethod
    def deserialize(cls, xbytes: bytes, config: Pi2LevConfig = None):
        if len(xbytes) != 2 * config.param_lambda:
            raise ValueError(
                "The length of xbytes must be 2 times the length of the parameter param_k."
            )

        K1, K2 = xbytes[:config.param_lambda], xbytes[config.param_lambda:]

        return cls(K1, K2, config)

    def __eq__(self, other):
        if not isinstance(other, Pi2LevToken):
            return False
        return self.K1 == other.K1 and self.K2 == other.K2


class Pi2LevResult(SSEResult):
    __slots__ = ["result"]

    def __init__(self, result: list, config: Pi2LevConfig = None):
        super(Pi2LevResult, self).__init__(config)
        self.result = result

    def serialize(self) -> bytes:
        return pickle.dumps(self.result)

    @classmethod
    def deserialize(cls, xbytes: bytes, config: Pi2LevConfig = None):
        result = pickle.loads(xbytes)
        if not isinstance(result, list):
            return ValueError("The data contained in xbytes is not a list.")

        return cls(result, config)

    def __str__(self):
        return self.result.__str__()

    def __eq__(self, other):
        if not isinstance(other, Pi2LevResult):
            return False
        return self.result == other.result

    def get_result_list(self) -> list:
        return self.result
