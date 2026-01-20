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

from gmn_adapter.config import Config
from gmn_adapter.exceptions import GMNAdapterDataPackageExists
from gmn_adapter.gmn.client import Client
from gmn_adapter.models.adapter.adapter_db import QueueManager
from gmn_adapter.models.pasta.package import Package


logger = daiquiri.getLogger(__name__)


def exists_in_gmn(package: Package) -> bool:
    """
    Check if a data package exists in GMN.

    Args:
        package (Package): PASTA data package to check.

    Returns:
        bool: True if all resources exist in GMN, False otherwise.

    Throws:
        RuntimeError: If a partial data package exists in GMN.
    """
    gmn_client = Client(node=Config.GMN_NODE)
    non_data_resource_count = 3
    exists = 0

    ore = package.doi
    if gmn_client.object_exists(ore):
        exists += 1

    metadata = package.resource_ids[Config.METADATA]
    if gmn_client.object_exists(metadata):
        exists += 1

    report = package.resource_ids[Config.REPORT]
    if gmn_client.object_exists(report):
        exists += 1

    for data in package.resource_ids[Config.DATA]:
        if gmn_client.object_exists(data):
            exists += 1

    if exists == non_data_resource_count + len(package.resource_ids[Config.DATA]):
        return True
    elif exists > 0:
        raise RuntimeError(f"A partial data package exists in GMN for {package.pid}.")
    else:
        return False

def create(package: Package) -> None:
    """Create a new data package in GMN."""
    pass


def update(predecessor: Package, package: Package) -> None:
    """Update an existing data package in GMN."""
    pass


def synchronize_to_gmn(package: Package, queue_manager: QueueManager, pasta_db_engine: Engine, dryrun: bool=True) -> None:
    """
    Synchronize the PASTA data package with GMN.

    Args:
        package (Package): PASTA data package to synchronize with GMN.
        queue_manager (QueueManager): Adapter queue manager.
        pasta_db_engine (Engine): SQLAlchemy engine instance of the PASTA database.
        dryrun (bool): If True, perform a dry run without making changes to GMN.

    Returns:
        None

    Throws:
        RuntimeError: If the package has a queued ancestor, or if a partial data package exists in GMN.
        GMNAdapterDataPackageExists: If the complete data package exists in GMN.
    """
    exists = exists_in_gmn(package=package)  # Raises RuntimeError only if a partial data package exists in GMN.
    if queue_manager.has_queued_ancestors(package.pid):
        # Ancestor package(s) must be synchronized first.
        raise RuntimeError(f"Package {package.pid} has a queued ancestor")
    elif exists:
        raise GMNAdapterDataPackageExists(f"Package {package.pid} already exists in GMN.")
    else:
        if predecessor := queue_manager.get_predecessor(package=package.pid):
            predecessor_pid = str(predecessor.package)
            predecessor = Package(pid=predecessor_pid, pasta_db_engine=pasta_db_engine)
            logger.info(f"Updating packages ({predecessor.pid}, {package.pid})")
            if not dryrun:
                update(predecessor, package)
        else:
            logger.info(f"Creating package {package.pid}")
            if not dryrun:
                create(package)
