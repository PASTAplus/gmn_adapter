#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Pytest configuration container in support of their implicit is better than explicit.

Module:
    conftest

Author:
    servilla

Created:
    2025-12-16
"""
import pytest

from gmn_adapter.db.adapter_db import QueueManager


@pytest.fixture(scope="session")
def queue_manager():
    return QueueManager("./data/adapter_queue.sqlite")
