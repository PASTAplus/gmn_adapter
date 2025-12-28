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
from sqlalchemy.exc import NoResultFound, IntegrityError

from gmn_adapter.config import Config
from gmn_adapter.model.adapter_db import QueueManager

import utils.sqlite_utils as su


logger = daiquiri.getLogger(__name__)

# Constants derived from tests/data/adapter_queue.csv
QUEUE_COUNT = 688
HEAD_PID = "knb-lter-atz.2.1"
LAST_DATETIME = datetime(2025, 12, 23, 18, 42, 12, 710000)
EVENT_PID ="edi.723.1"
EVENT_DATETIME = datetime(2021, 6, 7, 15, 33, 30, 627000)
EVENT_OWNER = "EDI-10f715defa6b12d4c9b4baba091a641c001dd2e4"
EVENT_DOI = "doi:10.0311/FK2/3e642c867749c87239e816547b001684"
EVENT_DEQUEUED = False
EVENT_INVALID_PID = "icarus.1.1"
DEQUEUED_PID = "knb-lter-cap.574.1"
DESCENDANT = "knb-lter-nope.1.2"
PREDECESSOR = "knb-lter-nope.1.1"


def test_new_queue_manager(queue_manager):
    """Test that the QueueManager class can be instantiated."""
    assert queue_manager is not None


def test_delete_queue(queue_manager):
    """Test that the SQLite database file can be deleted.

    Because the QueueManager class uses an in-memory SQLite database by default,
    we need to create a file-based database from the in-memory database for
    testing purposes. We then delete the file-based database and verify that
    it no longer exists.
    """
    file_db = Config.ROOT_DIR / "tests" / "data" / "adapter_queue.sqlite"
    su.sqlite_memory_to_file(queue_manager.engine, str(file_db))
    assert file_db.exists()
    qm = QueueManager(str(file_db))
    qm.delete_queue()
    assert not file_db.exists()


def test_enqueue(queue_manager, event):
    """Test that events can be enqueued into the adapter queue."""
    queue_manager.enqueue(event)
    last_event = queue_manager.get_last_event()
    assert last_event.package == event.package

    # Test duplicate event
    with pytest.raises(IntegrityError):
        queue_manager.enqueue(event)


def test_get_count(queue_manager):
    """Test that the queue count is correct."""
    count = queue_manager.get_count()
    assert count == QUEUE_COUNT


def test_get_event(queue_manager):
    """Test that the queue event can be retrieved by package identifier."""
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
    """Test that the queue head can be retrieved."""
    head = queue_manager.get_head()
    assert head.package == HEAD_PID


def test_get_last_datetime(queue_manager):
    """Test that the last event datetime can be retrieved."""
    last_datetime = queue_manager.get_last_datetime()
    assert last_datetime == LAST_DATETIME


def  test_get_predecessor(queue_manager):
    predecessor = queue_manager.get_predecessor(DESCENDANT)
    assert predecessor.package == PREDECESSOR

    # Test for the end of lineage
    predecessor = queue_manager.get_predecessor(PREDECESSOR)
    assert predecessor is None


def test_dequeue(queue_manager):
    """Test that events can be dequeued from the adapter queue."""
    queue_manager.dequeue(HEAD_PID)
    event = queue_manager.get_event(HEAD_PID)
    assert event.dequeued == True


def test_is_dequeued(queue_manager):
    """Test that an event is really dequeued."""
    dequeued = queue_manager.is_dequeued(DEQUEUED_PID)
    assert dequeued

    # Test that an active event is not dequeued
    dequeued = queue_manager.is_dequeued(HEAD_PID)
    assert not dequeued



