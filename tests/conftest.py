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
from gmn_adapter.model.adapter_db import Queue, QueueManager
from gmn_adapter.model.event import Event


@pytest.fixture(scope="function")
def queue_manager():
    """Load data package manager queue data from CSV into a memory-based SQLite database."""

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

    return qm


@pytest.fixture(scope="session")
def event():
    """Create a test event for use in tests."""

    PACKAGE = "knb-lter-nin.1.1"
    TIMESTAMP = datetime.fromisoformat("2025-12-26 12:34:56.2345")
    METHOD = "create"
    OWNER = "EDI-166ebf44ac70835c7ebce152e2219ae5eab16418"
    DOI = "doi:10.6073/pasta/0675d3602ff57f24838ca8d14d7f3961"

    return Event(
        package=PACKAGE,
        timestamp=TIMESTAMP,
        method=METHOD,
        owner=OWNER,
        doi=DOI,
    )