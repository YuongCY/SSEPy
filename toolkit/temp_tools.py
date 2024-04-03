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
@description: tools for create temp file path
"""

import tempfile
import os as _os
import _thread

from random import Random as _Random

_allocate_lock = _thread.allocate_lock

_TEMP_PREFIX = 'ssepy_temp_'


class _RandomNameSequence:
    """ from tempfile._RandomNameSequence of Python 3.11 """

    characters = "abcdefghijklmnopqrstuvwxyz0123456789_"

    @property
    def rng(self):
        cur_pid = _os.getpid()
        if cur_pid != getattr(self, '_rng_pid', None):
            self._rng = _Random()
            self._rng_pid = cur_pid
        return self._rng

    def __iter__(self):
        return self

    def __next__(self):
        return ''.join(self.rng.choices(self.characters, k=8))


_name_sequence = None
_once_lock = _allocate_lock()


def _get_candidate_names():
    """Common setup sequence for all user-callable interfaces.
    @note: from tempfile._get_candidate_names of Python 3.11
    """

    global _name_sequence
    if _name_sequence is None:
        _once_lock.acquire()
        try:
            if _name_sequence is None:
                _name_sequence = _RandomNameSequence()
        finally:
            _once_lock.release()
    return _name_sequence


def make_temp_path() -> str:
    """ Create a temporary file path """
    tempdir = tempfile.gettempdir()
    names = _get_candidate_names()
    for seq in names:
        path = _os.path.join(tempdir, _TEMP_PREFIX + seq)
        if not _os.path.exists(path):
            return path
    raise FileExistsError("No usable temporary filename found")
