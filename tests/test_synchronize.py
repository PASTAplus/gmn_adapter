#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:

Module:
    test_synchronize

Author:
    servilla

Created:
    2026-01-19
"""
import daiquiri
import pytest

from gmn_adapter.models.adapter.adapter_db import QueueManager
from gmn_adapter.models.pasta.package import Package
from gmn_adapter.models.pasta.pasta_db import get_pasta_db_engine
from gmn_adapter.syncing.synchronize import synchronize_to_gmn

logger = daiquiri.getLogger(__name__)


DESCENDANT = "knb-lter-hbr.84.9"
PACKAGE = "knb-lter-hbr.84.8"
PREDECESSOR = "knb-lter-hbr.84.7"


def test_synchronize_create(queue_manager):
    """Test that a new package is created in GMN."""
    pasta_db_engine = get_pasta_db_engine()
    predecessor = Package(PREDECESSOR, pasta_db_engine)
    synchronize_to_gmn(package=predecessor, queue_manager=queue_manager, pasta_db_engine=pasta_db_engine)


def test_synchronize_update(queue_manager):
    """Test that an existing package is updated in GMN."""
    queue_manager.dequeue(PREDECESSOR)
    pasta_db_engine = get_pasta_db_engine()
    package = Package(PACKAGE, pasta_db_engine)
    synchronize_to_gmn(package=package, queue_manager=queue_manager, pasta_db_engine=pasta_db_engine)


def test_synchronize_runtime_error(queue_manager):
    """Test that a RuntimeError is raised when attempting to synchronize a package that has queued predecessors."""
    pasta_db_engine = get_pasta_db_engine()
    package = Package(DESCENDANT, pasta_db_engine)
    with pytest.raises(RuntimeError):
        synchronize_to_gmn(package=package, queue_manager=queue_manager, pasta_db_engine=pasta_db_engine)
