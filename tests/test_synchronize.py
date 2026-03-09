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

from gmn_adapter.exceptions import GMNAdapterNonSynchronizedAncestor
from gmn_adapter.models.pasta.package import Package
from gmn_adapter.models.pasta.pasta_db import get_pasta_db_engine
from gmn_adapter.cli.synchronize import synchronize_to_gmn

logger = daiquiri.getLogger(__name__)


DESCENDANT = "edi.118.3"
PACKAGE = "edi.118.2"
PREDECESSOR = "edi.118.1"
EXISTS = "edi.134.11"


def test_synchronize_create(queue_manager, gmn_client, config):
    """Test that a new package is created in GMN."""
    pasta_db_engine = get_pasta_db_engine(host=config["db_host"], port=config["db_port"])
    predecessor = Package(PREDECESSOR, pasta_db_engine)
    predecessor.ensure_resources_loaded()
    synchronize_to_gmn(package=predecessor, queue_manager=queue_manager, gmn_client=gmn_client, pasta_db_engine=pasta_db_engine, dryrun=True)


def test_synchronize_update(queue_manager, gmn_client, config):
    """Test that an existing package is updated in GMN."""
    queue_manager.dequeue(PREDECESSOR)
    pasta_db_engine = get_pasta_db_engine(host=config["db_host"], port=config["db_port"])
    package = Package(PACKAGE, pasta_db_engine)
    package.ensure_resources_loaded()
    synchronize_to_gmn(package=package, queue_manager=queue_manager, gmn_client=gmn_client, pasta_db_engine=pasta_db_engine, dryrun=True)


def test_synchronize_runtime_error(queue_manager, gmn_client, config):
    """Test that a RuntimeError is raised when attempting to synchronize a package that has queued predecessors."""
    pasta_db_engine = get_pasta_db_engine(host=config["db_host"], port=config["db_port"])
    package = Package(DESCENDANT, pasta_db_engine)
    package.ensure_resources_loaded()
    with pytest.raises(GMNAdapterNonSynchronizedAncestor):
        synchronize_to_gmn(package=package, queue_manager=queue_manager, gmn_client=gmn_client, pasta_db_engine=pasta_db_engine, dryrun=True)
