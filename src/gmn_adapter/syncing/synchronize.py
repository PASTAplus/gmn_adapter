#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:
    Synchronize the PASTA data package with GMN.

Module:
    synchronize

Author:
    servilla

Created:
    2026-01-19
"""
import daiquiri
from sqlalchemy import Engine

from gmn_adapter.models.adapter.adapter_db import QueueManager
from gmn_adapter.models.pasta.package import Package


logger = daiquiri.getLogger(__name__)


def create(package: Package) -> None:
    """Create a new data package in GMN."""
    pass


def update(predecessor: Package, package: Package) -> None:
    """Update an existing data package in GMN."""
    pass


def synchronize_to_gmn(package: Package, queue_manager: QueueManager, pasta_db_engine: Engine) -> None:
    """
    Synchronize the PASTA data package with GMN.

    Args:
        package (Package): PASTA data package to synchronize with GMN.
        queue_manager (QueueManager): Adapter queue manager.
        pasta_db_engine (Engine): SQLAlchemy engine instance of the PASTA database.:

    Returns:

    """
    if queue_manager.has_queued_ancestors(package.pid):
        # Ancestor package(s) must be synchronized first.
        raise RuntimeError(f"Package {package.pid} has a queued ancestor")
    else:
        if predecessor := (queue_manager.get_predecessor(package=package.pid)):
            predecessor_pid = str(predecessor.package)
            predecessor = Package(pid=predecessor_pid, pasta_db_engine=pasta_db_engine)
            logger.info(f"Updating packages ({predecessor.pid}, {package.pid})")
            update(predecessor, package)
        else:
            logger.info(f"Creating package {package.pid}")
            create(package)
