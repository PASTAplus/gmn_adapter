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
import csv
import pytest
from sqlalchemy import insert

from gmn_adapter.config import Config
from gmn_adapter.db.adapter_db import Queue, QueueManager


@pytest.fixture(scope="session")
def queue_manager():
    data = Config.ROOT_DIR / "tests" / "data" / "adapter_queue.csv"
    with open(data, "r") as f:
        reader = csv.DictReader(f)
        data = [row for row in reader]

    qm = QueueManager(":memory:")
    stmt = insert(Queue).values(data)
    qm.session.execute(stmt)
    qm.session.commit()

    yield qm
