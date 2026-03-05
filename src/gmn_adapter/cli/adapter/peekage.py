#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Peek at data package resources in GMN.

Module:
    peekage

Author:
    servilla

Created:
    2026-03-03
"""
import sys

import click
import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.exceptions import GMNAdapterPackageIsNotGMNCandidate, GMNAdapterDataPackageNotFound
from gmn_adapter.gmn.client import Client
from gmn_adapter.models.adapter.adapter_db import QueueManager
from gmn_adapter.models.pasta.package import Package
from gmn_adapter.models.pasta.resource_map import ResourceMap
from gmn_adapter.models.pasta.resource_type import ResourceType
from gmn_adapter.models.pasta.pasta_db import get_pasta_db_engine


logger = daiquiri.getLogger(__name__)


help_verbose = "Send output to standard out (-v or -vv or -vvv for increasing output)."
help_sysmeta = "Display sysmeta for each resource."


@click.command()
@click.argument("pid", type=str)
@click.option("--sysmeta", is_flag=True, default=False, help=help_sysmeta)
@click.option("-v", "--verbose", count=True, help=help_verbose)
@click.pass_context
def peekage(ctx, pid: str, sysmeta: bool, verbose: int):
    """
    Peek at data package resources in GMN.

    PID: PASTA data package identifier to peek on.
    """
    gmn_client = Client(node=Config.GMN_NODE)

    pasta_db_engine = get_pasta_db_engine()
    queue_manager = QueueManager(Config.QUEUE)
    package = None
    try:
        package = Package(pid=pid, pasta_db_engine=pasta_db_engine)
    except GMNAdapterDataPackageNotFound:
        if verbose > 0:
            click.echo(f"Data package \"{pid}\" was not found on PASTA.")
        logger.warning(f"Data package \"{pid}\" was not found on PASTA.")
        sys.exit(1)
    except GMNAdapterPackageIsNotGMNCandidate as e:
        if verbose > 0:
            click.echo(f"Cannot access resources for \"{pid}\".")
        sys.exit(1)

    resources = {}
    if package is not None:
        ore_create_date = None
        for resource in package.resources:
            if resource[ResourceMap.RESOURCE_TYPE] is ResourceType.DATA_PACKAGE:
                ore_create_date = resource[ResourceMap.DATE_CREATED]
            else:
                resource_peeks = {}
                resource_id = resource[ResourceMap.RESOURCE_ID]

                if gmn_client.object_exists(resource_id):
                    system_metadata = gmn_client.get_system_metadata(resource_id)
                    resource_peeks["sync_date"] = system_metadata.date_uploaded
                    resource_peeks["system_metadata"] = system_metadata
                else:
                    resource_peeks["sync_date"] = None
                    resource_peeks["system_metadata"] = None

                if resource[ResourceMap.RESOURCE_TYPE] is ResourceType.ORE:
                    resource_peeks["create_date"] = ore_create_date
                else:
                    resource_peeks["create_date"] = resource[ResourceMap.DATE_CREATED]
                resources[resource_id] = resource_peeks

        # Print out basic resource table
        key_width = max(len(key) for key in resources.keys())
        click.echo(f"{'Resource_ID':<{key_width}} | PASTA create datetime (UTC)  | GMN sync datetime (UTC)")
        for resource_id, peeks in resources.items():
            click.echo(f"{resource_id:<{key_width}} | {peeks['create_date']}Z  | {peeks['sync_date'].isoformat().replace('+00:00', 'Z')}")

        # Print out resource system metadata
        if sysmeta:
            for resource_id, peeks in resources.items():
                click.echo("\n\n")
                click.echo(f"{resource_id}:")
                click.echo(f"{peeks['system_metadata'].model_dump_json(indent=4, exclude_none=True)}")

    sys.exit(0)