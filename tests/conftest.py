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
from datetime import datetime

import pytest
from sqlalchemy import insert

from gmn_adapter.config import Config
from gmn_adapter.db.adapter_db import Queue, QueueManager


@pytest.fixture(scope="session")
def queue_manager():
    '''Load data package manager queue data from CSV into a memory-based SQLite database.'''

    data_path = Config.ROOT_DIR / "tests" / "data" / "adapter_queue.csv"
    data = []
    with open(data_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["identifier"] = int(row["identifier"])
            row["revision"] = int(row["revision"])
            row["datetime"] = datetime.fromisoformat(row["datetime"])
            row["dequeued"] = bool(int(row["dequeued"]))
            data.append(row)

    qm = QueueManager(":memory:")
    stmt = insert(Queue).values(data)
    qm.session.execute(stmt)
    qm.session.commit()

    yield qm
