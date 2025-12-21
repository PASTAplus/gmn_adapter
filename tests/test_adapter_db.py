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


logger = daiquiri.getLogger(__name__)


def test_new_queue_manager(queue_manager):
    assert queue_manager is not None


def test_queue_manager_count(queue_manager):
    count = queue_manager.get_count()
    assert count == 530
