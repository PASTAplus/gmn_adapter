#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Test module for the GMN Adapter adapter database model, and specifically the QueueManager class.

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
from gmn_adapter.models.adapter.adapter_db import QueueManager, DuplicateQueueEntryError

import utils.sqlite_utils as su


logger = daiquiri.getLogger(__name__)

# Constants derived from tests/data/adapter_queue.csv
QUEUE_COUNT = 1099
HEAD_PID = "knb-lter-arc.1008.8"
TAIL_PID = "knb-lter-cap.727.1"
NEWEST_PID = "knb-lter-cap.653.3"
NEWEST_DATETIME = datetime(2025, 7, 23, 20, 30, 53, 241000)
EVENT_PID ="knb-lter-gce.101.42"
EVENT_DATETIME = datetime(2019, 10, 28, 14, 25, 24, 496000)
EVENT_OWNER = "EDI-abdb1ad3e4f1715fb1994b49e15a4c7d40b98ae6"
EVENT_DOI = "doi:10.0311/FK2/bc7949668fe934c4d7e1e99a219a5566"
EVENT_DEQUEUED = False
EVENT_INVALID_PID = "icarus.1.1"
DEQUEUED_PID = "knb-lter-cap.574.1"
DESCENDANT = "knb-lter-hbr.84.9"
PREDECESSOR = "knb-lter-hbr.84.7"


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
    with pytest.raises(DuplicateQueueEntryError):
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


def test_get_tail(queue_manager):
    """Test that the queue tail can be retrieved."""
    tail = queue_manager.get_tail()
    assert tail.package == TAIL_PID


def test_get_newest_event(queue_manager):
    """Test that the newest event can be retrieved."""
    newest_event = queue_manager.get_newest_event()
    assert newest_event.package == NEWEST_PID
    newest_event_datetime = newest_event.datetime
    assert newest_event_datetime == NEWEST_DATETIME
    print(newest_event_datetime.isoformat())


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


def test_set_dirty(queue_manager):
    """Test that an event is marked as dirty."""
    queue_manager.set_dirty(HEAD_PID)
    head = queue_manager.get_head(clean=False)
    assert head.dirty == True


def test_set_all_clean(queue_manager):
    """Test that all events are marked as clean."""
    queue_manager.set_dirty(HEAD_PID)
    head = queue_manager.get_head(clean=False)
    assert head.dirty == True

    queue_manager.set_all_clean()

    pid = head.package
    package = queue_manager.get_event(package=pid)
    assert package.dirty == False


def test_has_queued_ancestors(queue_manager):
    """Test that has_queued_ancestors returns correct results."""
    has_ancestors = queue_manager.has_queued_ancestors(DESCENDANT)
    assert has_ancestors

    has_ancestors = queue_manager.has_queued_ancestors(PREDECESSOR)
    assert not has_ancestors




