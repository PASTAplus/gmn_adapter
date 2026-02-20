#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Retrieve and update system metadata for a given MN and PID.

Module:
    sysmeta

Author:
    servilla

Created:
    2026-01-06
"""
import sys

import click
import daiquiri
import d1_common.types.exceptions as d1_exceptions

from gmn_adapter.config import Config
from gmn_adapter.gmn.client import Client
from gmn_adapter.models.dataone.sysmeta import SysMeta


logger = daiquiri.getLogger(__name__)


help_full = "Display full system metadata, including empty fields."
help_update = "Update system metadata from JSON file."
help_verify = "Verify system metadata has been updated successfully."

@click.command()
@click.argument("pid", type=str)
@click.option("-f", "--full", is_flag=True, default=False, help=help_full)
@click.option("-u", "--update", type=click.Path(exists=True), help=help_update,)
@click.option("-v", "--verify", is_flag=True, default=False, help=help_verify)
@click.pass_context
def sysmeta(ctx, pid: str, full: bool, update: str, verify: bool):
    """
    Retrieve and update system metadata for the system GMN and PID.

    PID: Persistent Identifier (e.g., "https://pasta.lternet.edu/package/metadata/eml/edi/1/1").
    """

    # Test to exclude any empty JSON field
    exclude_empty = True
    if full:
        exclude_empty = False

    gmn_client = Client(Config.GMN_NODE)

    # Update system metadata from the JSON file, then exit.
    if update:
        with open(update, "r") as f:
            sys_meta: SysMeta = SysMeta.model_validate_json(f.read())
        click.echo(sys_meta.model_dump_json(indent=4, exclude_none=exclude_empty))
        click.echo("\n")
        click.confirm("Update system metadata?", abort=True)
        click.echo(f"Updating system metadata for \"{Config.GMN_NODE}: {pid}\".")
        logger.info(f"Updating system metadata for \"{Config.GMN_NODE}: {pid}\".")
        try:
            gmn_client.update_system_metadata(pid, sys_meta)
        except d1_exceptions.NotFound as e:
            msg = f"System metadata not found for \"{pid}\" on the \"{Config.GMN_NODE}\" GMN."
            click.echo(msg)
            logger.info(msg)
            sys.exit(1)

        if verify:
            sys_meta: SysMeta = gmn_client.get_system_metadata(pid)
            click.echo("\n")
            click.echo(sys_meta.model_dump_json(indent=4, exclude_none=exclude_empty))
        sys.exit(0)

    # Read and display system metadata, then exit.
    try:
        sys_meta: SysMeta = gmn_client.get_system_metadata(pid)
    except d1_exceptions.NotFound as e:
        msg = f"System metadata not found for \"{pid}\" on the \"{Config.GMN_NODE}\" GMN."
        click.echo(msg)
        logger.info(msg)
        sys.exit(1)

    logger.info(f"Dumping system metadata for \"{Config.GMN_NODE}: {pid}\".")
    click.echo(sys_meta.model_dump_json(indent=4, exclude_none=exclude_empty))
    sys.exit(0)
