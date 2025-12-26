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

import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.db.adapter_db import QueueManager

import utils.sqlite_utils as su


logger = daiquiri.getLogger(__name__)


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


def test_dequeue(queue_manager):
    pass


def test_enqueue(queue_manager):
    pass


def test_get_count(queue_manager):
    """Test that the queue count is correct.

    The test queue contains 530 records.
    """
    count = queue_manager.get_count()
    assert count == 530


def test_get_event(queue_manager):
    pass


def test_get_head(queue_manager):
    pass


def test_get_last_datetime(queue_manager):
    pass


def  test_get_predecessor(queue_manager):
    pass


def test_is_dequeued(queue_manager):
    pass



