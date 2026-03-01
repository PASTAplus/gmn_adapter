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


def exists_in_gmn(package: Package, gmn_client: Client, dryrun: bool=False, verbose: int=0) -> bool:
    """
    Check if a data package exists in GMN.

    Args:
        package (Package): PASTA data package to check.
        gmn_client (Client): GMN client.
        dryrun (bool, optional): If True, perform a dry run without actually checking the object existence. Defaults to False.
        verbose (int): Verbosity level for logging. Default is 0.

    Returns:
        bool: True if all resources exist in GMN, False otherwise.

    Throws:
        GMNAdapterPartialDataPackageExists: If a partial data package exists in GMN.
    """
    if dryrun: return False

    missing_resources = []
    object_count = 0
    for resource in package.resources:
        if resource[ResourceMap.RESOURCE_TYPE] != ResourceType.DATA_PACKAGE:
            if gmn_client.object_exists(resource[ResourceMap.RESOURCE_ID]):
                object_count += 1
            else:
                missing_resources.append(resource[ResourceMap.RESOURCE_ID])

    if object_count == len(package.resources) - 1:
        return True
    elif object_count == 0:
        return False
    else:
        msg = f"A partial data package exists in GMN for {package.pid}."
        raise GMNAdapterPartialDataPackageExists(msg, missing_resources)


def create(package: Package, gmn_client: Client, repair: bool=False, dryrun: bool=False, verbose: int=0) -> None:
    """Create a new data package in GMN."""
    for resource in package.resources:
        if resource[ResourceMap.RESOURCE_TYPE] != ResourceType.DATA_PACKAGE:
            sys_meta = system_metadata_factory(package_id=package.pid, replication_policy=package.replication_policy, resource=resource)
            if resource[ResourceMap.RESOURCE_TYPE] == ResourceType.ORE:
                data = package.ore
                pass_through_url = None
            else:
                data = None
                pass_through_url = resource[ResourceMap.RESOURCE_ID]

            gmn_client.create_object(
                pid=resource[ResourceMap.RESOURCE_ID],
                sys_meta=sys_meta,
                data=data,
                pass_through_url=pass_through_url,
                dryrun=dryrun
            )



def update(predecessor: Package, package: Package, gmn_client: Client, repair: bool=False, dryrun: bool=False, verbose: int=0) -> None:
    """Update an existing data package in GMN."""
    predecessor_ore_pid = predecessor_metadata_pid = None
    for resource in predecessor.resources:
        match resource[ResourceMap.RESOURCE_TYPE]:
            case ResourceType.ORE:
                predecessor_ore_pid = resource[ResourceMap.RESOURCE_ID]
            case ResourceType.METADATA:
                predecessor_metadata_pid = resource[ResourceMap.RESOURCE_ID]
            case _:
                continue

    for resource in package.resources:
        if resource[ResourceMap.RESOURCE_TYPE] != ResourceType.DATA_PACKAGE:
            sys_meta = system_metadata_factory(package_id=package.pid, replication_policy=package.replication_policy, resource=resource)
            if resource[ResourceMap.RESOURCE_TYPE] == ResourceType.ORE:
                # Use update to build obsolescence chain for ORE
                gmn_client.update_object(
                    predecessor_pid=predecessor_ore_pid,
                    pid=resource[ResourceMap.RESOURCE_ID],
                    sys_meta=sys_meta,
                    data=package.ore,
                    pass_through_url=None,
                    dryrun=dryrun
                )
            elif resource[ResourceMap.RESOURCE_TYPE] == ResourceType.METADATA:
                # Use update to build obsolescence chain for METADATA
                gmn_client.update_object(
                    predecessor_pid=predecessor_metadata_pid,
                    pid=resource[ResourceMap.RESOURCE_ID],
                    sys_meta=sys_meta,
                    data=None,
                    pass_through_url=resource[ResourceMap.RESOURCE_ID],
                    dryrun=dryrun
                )
            else:
                # REPORT and DATA do not have obsolescence chains
                gmn_client.create_object(
                    pid=resource[ResourceMap.RESOURCE_ID],
                    sys_meta=sys_meta,
                    data=None,
                    pass_through_url=resource[ResourceMap.RESOURCE_ID],
                    dryrun=dryrun
                )


def synchronize_to_gmn(
    package: Package,
    queue_manager: QueueManager,
    pasta_db_engine: Engine,
    gmn_client: Client,
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
        gmn_client (Client): GMN client instance.
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
        if exists_in_gmn(package=package, gmn_client=gmn_client, dryrun=dryrun):  # re-throws GMNAdapterPartialDataPackageExists
            raise GMNAdapterDataPackageExists(f"Package \"{package.pid}\" already exists in \"{Config.GMN_NODE}\" GMN.")
    except GMNAdapterPartialDataPackageExists as e:
        if not repair:
            raise e

    if (predecessor := queue_manager.get_predecessor(package=package.pid)) is not None:
        predecessor_pid = str(predecessor.package)
        predecessor = Package(pid=predecessor_pid, pasta_db_engine=pasta_db_engine)
        # Ensure all resources are loaded for predecessor before update
        predecessor.ensure_resources_loaded()
        if verbose > 0:
            click.echo(f"Updating packages ({predecessor.pid}, {package.pid})")
        logger.info(f"Updating packages ({predecessor.pid}, {package.pid})")
        if not dryrun:
            update(predecessor, package, gmn_client, repair, dryrun, verbose)
    else:
        if verbose > 0:
            click.echo(f"Creating package {package.pid}")
        logger.info(f"Creating package {package.pid}")
        if not dryrun:
            create(package, gmn_client, repair, dryrun, verbose)
