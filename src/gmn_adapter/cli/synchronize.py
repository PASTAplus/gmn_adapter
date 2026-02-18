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
from enum import IntFlag

import daiquiri
from sqlalchemy import Engine

from gmn_adapter.config import Config
from gmn_adapter.exceptions import GMNAdapterDataPackageExists, GMNAdapterPartialDataPackageExists, GMNAdapterNonSynchronizedAncestor
from gmn_adapter.gmn.client import Client
from gmn_adapter.models.adapter.adapter_db import QueueManager
from gmn_adapter.models.pasta.package import Package


logger = daiquiri.getLogger(__name__)


class packageStatus(IntFlag):
    """Enumeration of package status flags."""
    EMPTY = 0
    ORE = 1
    METADATA = 2
    REPORT = 4
    DATA = 8


def exists_in_gmn(package: Package, verbose: int=0) -> bool:
    """
    Check if a data package exists in GMN.

    Args:
        package (Package): PASTA data package to check.
        verbose (int): Verbosity level for logging. Default is 0.

    Returns:
        bool: True if all resources exist in GMN, False otherwise.

    Throws:
        GMNAdapterPartialDataPackageExists: If a partial data package exists in GMN.
    """
    gmn_client = Client(node=Config.GMN_NODE)

    status = packageStatus.EMPTY
    complete = packageStatus.ORE | packageStatus.METADATA | packageStatus.REPORT | packageStatus.DATA
    missing_resources = []

    ore = package.doi
    if gmn_client.object_exists(ore):
        status = packageStatus.ORE
    else:
        missing_resources.append(ore)

    metadata = package.resource_ids[Config.METADATA]
    if gmn_client.object_exists(metadata):
        status = status | packageStatus.METADATA
    else:
        missing_resources.append(metadata)

    report = package.resource_ids[Config.REPORT]
    if gmn_client.object_exists(report):
        status = status | packageStatus.REPORT
    else:
        missing_resources.append(report)

    all_data = True
    for data in package.resource_ids[Config.DATA]:
        if not gmn_client.object_exists(data):
            all_data = False
            missing_resources.append(data)
    if all_data:
        status = status | packageStatus.DATA

    if status == complete:
        return True
    elif status == packageStatus.EMPTY:
        return False
    else:
        msg = f"A partial data package exists in GMN for {package.pid}."
        raise GMNAdapterPartialDataPackageExists(msg, missing_resources)


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
        RuntimeError: If the package has a queued ancestor.
        GMNAdapterPartialDataPackageExists: If a partial data package exists in GMN.
        GMNAdapterDataPackageExists: If the complete data package exists in GMN.
    """
    if queue_manager.has_queued_ancestors(package.pid):
        # Ancestor package(s) must be synchronized first.
        raise GMNAdapterNonSynchronizedAncestor(f"Package {package.pid} has a non-synchronized ancestor")

    if exists_in_gmn(package=package):  # re-throws GMNAdapterPartialDataPackageExists
        raise GMNAdapterDataPackageExists(f"Package \"{package.pid}\" already exists in \"{Config.GMN_NODE}\" GMN.")

    if (predecessor := queue_manager.get_predecessor(package=package.pid)) is not None:
        predecessor_pid = str(predecessor.package)
        predecessor = Package(pid=predecessor_pid, pasta_db_engine=pasta_db_engine)
        logger.info(f"Updating packages ({predecessor.pid}, {package.pid})")
        if not dryrun:
            update(predecessor, package)
    else:
        logger.info(f"Creating package {package.pid}")
        if not dryrun:
            create(package)
