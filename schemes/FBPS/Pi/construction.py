# -*- coding:utf-8 _*-
""" 
FBPS CODE
@author: Yuhong Zhang
@license: GPL-3.0 License
@file: construction.py
@time: 2024/07/06
@contact: yuongys@163.com
@site:  
@software: PyCharm 
@description: 
"""
import os

import schemes.interface.inverted_index_sse
from schemes.FBPS.Pi.config import DEFAULT_CONFIG, PiConfig
from schemes.FBPS.Pi.structures import PiKey, PiToken, PiEncryptedDatabase, PiResult
from toolkit.bytes_utils import int_to_bytes


class Pi(schemes.interface.inverted_index_sse.InvertedIndexSSE):
    """Pi Construction described by Cash et al. [CJJ+14]"""

    def __init__(self, config: dict = DEFAULT_CONFIG):
        super(Pi, self).__init__()
        self.config = PiConfig(config)
        pass

    def _Gen(self) -> PiKey:
        """
        Generate Key
        K2 is not used here now.
        """
        K = os.urandom(self.config.param_lambda)
        return PiKey(K)

    def _Enc(self, K: PiKey, database: dict) -> PiEncryptedDatabase:
        """Encrypted the given database under the key"""
        K = K.K
        L = []

        for keyword in database:
            K1 = self.config.prf_f(K, b'\x01' + keyword)
            K2 = self.config.prf_f(K, b'\x02' + keyword)
            for c, identifier in enumerate(database[keyword]):
                l = self.config.prf_f(K1, int_to_bytes(c))
                d = self.config.ske.Encrypt(K2, identifier)
                L.append((l, d))
        return PiEncryptedDatabase.build_from_list(L)

    def _Trap(self, K: PiKey, keyword: bytes) -> PiToken:
        """Trapdoor Generation Algorithm"""
        K = K.K
        K1 = self.config.prf_f(K, b'\x01' + keyword)
        K2 = self.config.prf_f(K, b'\x02' + keyword)
        return PiToken(K1, K2)

    def _Search(self, edb: PiEncryptedDatabase, tk: PiToken) -> PiResult:
        """Search Algorithm"""
        D = edb.D
        K1, K2 = tk.K1, tk.K2
        result = []
        c = 0
        while True:
            addr = self.config.prf_f(K1, int_to_bytes(c))
            cipher = D.get(addr)
            if cipher is None:
                break
            result.append(self.config.ske.Decrypt(K2, cipher))
            c += 1

        return PiResult(result)

    def KeyGen(self) -> PiKey:
        key = self._Gen()
        return key

    def EDBSetup(self,
                 key: PiKey,
                 database: dict
                 ) -> PiEncryptedDatabase:
        return self._Enc(key, database)

    def TokenGen(self, key: PiKey, keyword: bytes) -> PiToken:
        return self._Trap(key, keyword)

    def Search(self,
               edb: PiEncryptedDatabase,
               token: PiToken) -> PiResult:
        return self._Search(edb, token)