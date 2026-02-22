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
import click
import daiquiri
from sqlalchemy import Engine

from gmn_adapter.cli.system_metadata import system_metadata_factory
from gmn_adapter.config import Config
from gmn_adapter.exceptions import GMNAdapterDataPackageExists, GMNAdapterPartialDataPackageExists, GMNAdapterNonSynchronizedAncestor
from gmn_adapter.gmn.client import Client
from gmn_adapter.models.adapter.adapter_db import QueueManager
from gmn_adapter.models.pasta.package import Package
from gmn_adapter.models.pasta.resource_map import ResourceMap
from gmn_adapter.models.pasta.resource_type import ResourceType


logger = daiquiri.getLogger(__name__)


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

    missing_resources = []
    object_count = 0
    for resource in package.resources:
        if resource[ResourceMap.RESOURCE_TYPE.value] != ResourceType.DATA_PACKAGE:
            if gmn_client.object_exists(resource[ResourceMap.RESOURCE_ID.value]):
                object_count += 1
            else:
                missing_resources.append(resource[ResourceMap.RESOURCE_ID.value])

    if object_count == len(package.resources) - 1:
        return True
    elif object_count == 0:
        return False
    else:
        msg = f"A partial data package exists in GMN for {package.pid}."
        raise GMNAdapterPartialDataPackageExists(msg, missing_resources)


def create(package: Package, repair: bool=False, verbose: int=0) -> None:
    """Create a new data package in GMN."""
    print(package)
    for resource in package.resources:
        if resource[ResourceMap.RESOURCE_TYPE.value] != ResourceType.DATA_PACKAGE:
            sysmeta = system_metadata_factory(package_id=package.pid, replication_policy=package.replication_policy, resource=resource)


def update(predecessor: Package, package: Package, repair: bool=False, verbose: int=0) -> None:
    """Update an existing data package in GMN."""
    print(predecessor)
    print(package)


def synchronize_to_gmn(
    package: Package,
    queue_manager: QueueManager,
    pasta_db_engine: Engine,
    repair: bool=False,
    dryrun: bool=False,
    verbose: int=0
) -> None:
    """
    Synchronize the PASTA data package with GMN.

    Args:
        package (Package): PASTA data package to synchronize with GMN.
        queue_manager (QueueManager): Adapter queue manager.
        pasta_db_engine (Engine): SQLAlchemy engine instance of the PASTA database.
        repair (bool): If True, attempt to repair an incomplete data package in the GMN.
        dryrun (bool): If True, perform a dry run without making changes to GMN.
        verbose (int): Verbosity level.

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

    try:
        if exists_in_gmn(package=package):  # re-throws GMNAdapterPartialDataPackageExists
            raise GMNAdapterDataPackageExists(f"Package \"{package.pid}\" already exists in \"{Config.GMN_NODE}\" GMN.")
    except GMNAdapterPartialDataPackageExists as e:
        if not repair:
            raise e

    if (predecessor := queue_manager.get_predecessor(package=package.pid)) is not None:
        predecessor_pid = str(predecessor.package)
        predecessor = Package(pid=predecessor_pid, pasta_db_engine=pasta_db_engine)
        if verbose > 0:
            click.echo(f"Updating packages ({predecessor.pid}, {package.pid})")
        logger.info(f"Updating packages ({predecessor.pid}, {package.pid})")
        if not dryrun:
            update(predecessor, package, repair, verbose)
    else:
        if verbose > 0:
            click.echo(f"Creating package {package.pid}")
        logger.info(f"Creating package {package.pid}")
        if not dryrun:
            create(package, repair, verbose)
