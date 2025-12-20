#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Test module for the GMN Adapter adapter database model.

Module:
    test_adapter_db

Author:
    servilla

Created:
    2025-12-16
"""

import daiquiri
import pytest

from gmn_adapter.config import Config


logger = daiquiri.getLogger(__name__)

print(Config.QUEUE)
pass
