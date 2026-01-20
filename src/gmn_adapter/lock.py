#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Simple file-based mutex lock.

Module:
    lock
 
Author:
    servilla

Created:
    2026-01-20
"""
import string
from pathlib import Path
import random

import daiquiri


logger = daiquiri.getLogger(__name__)


class Lock(object):

    def __init__(self, file_name=None):

        if file_name is None:
            random_str = lambda n: ''.join([random.choice(
                string.ascii_letters) for i in range(n)])
            self._file_name = random_str(10) + ".lock"
        else:
            self._file_name = file_name

    def acquire(self):
        Path(self._file_name).touch()

    def release(self):
        Path(self._file_name).unlink()

    @property
    def locked(self):
        return Path(self._file_name).exists()

    @property
    def lock_file(self):
        return self._file_name
