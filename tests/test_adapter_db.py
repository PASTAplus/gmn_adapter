#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Test module for the GMN Adapter adapter database model, and specifically
the QueueManager class.

Module:
    test_adapter_db

Author:
    servilla

Created:
    2025-12-16
"""
from datetime import datetime

import daiquiri
import pytest
from sqlalchemy.exc import NoResultFound

from gmn_adapter.config import Config
from gmn_adapter.model.adapter_db import QueueManager

import utils.sqlite_utils as su


logger = daiquiri.getLogger(__name__)

# Constants derived from adapter_queue.csv
QUEUE_COUNT = 530
HEAD_PID = "knb-lter-fce.1084.11"
LAST_DATETIME = datetime(2023, 6, 15, 19, 2, 38, 596000)
EVENT_PID ="knb-lter-hfr.437.1"
EVENT_DATETIME = datetime(2023, 6, 15, 12, 3, 21, 159000)
EVENT_METHOD = "createDataPackage"
EVENT_OWNER = "uid=HFR,o=EDI,dc=edirepository,dc=org"
EVENT_DOI = "doi:10.6073/pasta/6b2cd2ae330c7ea45d46cc553244fa81"
EVENT_DEQUEUED = False
EVENT_INVALID_PID = "icarus.1.1"
DEQUEUED_PID = "knb-lter-hfr.3.31"


def test_new_queue_manager(queue_manager):
    """Test that the QueueManager class can be instantiated."""
    assert queue_manager is not None


def test_delete_queue(queue_manager):
    """Test that the SQLite database file can be deleted.

    Because the QueueManager class uses an in-memory SQLite database by default,
    we need to create a file-based database for testing purposes. We then delete
    the file-based database and verify that it no longer exists.
    """
    file_db = Config.ROOT_DIR / "tests" / "data" / "adapter_queue.sqlite"
    su.sqlite_memory_to_file(queue_manager.engine, str(file_db))
    assert file_db.exists()
    qm = QueueManager(str(file_db))
    qm.delete_queue()
    assert not file_db.exists()


def test_enqueue(queue_manager, event):
    pass


def test_get_count(queue_manager):
    """Test that the queue count is correct."""
    count = queue_manager.get_count()
    assert count == QUEUE_COUNT


def test_get_event(queue_manager):
    # Test valid event PID
    event = queue_manager.get_event(EVENT_PID)
    assert event.package == EVENT_PID
    assert event.datetime == EVENT_DATETIME
    assert event.owner == EVENT_OWNER
    assert event.doi == EVENT_DOI
    assert event.dequeued == EVENT_DEQUEUED

    # Test invalid event PID
    with pytest.raises(NoResultFound):
        queue_manager.get_event(EVENT_INVALID_PID)


def test_get_head(queue_manager):
    head = queue_manager.get_head()
    assert head.package == HEAD_PID


def test_get_last_datetime(queue_manager):
    last_datetime = queue_manager.get_last_datetime()
    assert last_datetime == LAST_DATETIME


def  test_get_predecessor(queue_manager):
    pass


def test_dequeue(queue_manager):
    queue_manager.dequeue(HEAD_PID)
    event = queue_manager.get_event(HEAD_PID)
    assert event.dequeued == True


def test_is_dequeued(queue_manager):
    dequeued = queue_manager.is_dequeued(DEQUEUED_PID)
    assert dequeued

    dequeued = queue_manager.is_dequeued(HEAD_PID)
    assert not dequeued



