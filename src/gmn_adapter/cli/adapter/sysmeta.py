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

from gmn_adapter.gmn.client import Client
from gmn_adapter.models.dataone.sysmeta import SysMeta


logger = daiquiri.getLogger(__name__)


@click.command()
@click.argument("mn", type=str)
@click.argument("pid", type=str)
@click.option("-f", "--full", is_flag=True, default=False,
              help="Display full system metadata, including empty fields.")
@click.option("-u", "--update", type=click.Path(exists=True),
              help="Update system metadata from JSON file.")
@click.option("-v", "--verify", is_flag=True, default=False,
              help="Verify system metadata has been updated successfully.")
@click.pass_context
def sysmeta(ctx, mn: str, pid: str, full: bool, update: str, verify: bool):
    """
    Retrieve and update system metadata for a given MN and PID.

    MN: Member Node identifier (EDI or LTER).\n
    PID: Persistent Identifier (e.g., "https://pasta.lternet.edu/package/metadata/eml/edi/1/1").
    """
    if mn not in ("EDI", "LTER"):
        sys.stderr.write(f"Invalid MN identifier: {mn}. Must be either 'EDI' or 'LTER'.")
        exit(1)

    # Test to exclude any JSON field that contains a null (None) value
    exclude_none = True
    if full:
        exclude_none = False

    gmn_client = Client(mn)

    # Update system metadata from the JSON file, then exit.
    if update:
        with open(update, "r") as f:
            sys_meta: SysMeta = SysMeta.model_validate_json(f.read())
        print(sys_meta.model_dump_json(indent=4, exclude_none=exclude_none))
        print("\n")
        click.confirm("Update system metadata?", abort=True)
        click.echo(f"Updating system metadata for \"{mn}: {pid}\".")
        logger.info(f"Updating system metadata for \"{mn}: {pid}\".")
        try:
            gmn_client.update_system_metadata(pid, sys_meta)
        except d1_exceptions.NotFound as e:
            msg = f"System metadata not found for \"{pid}\" on \"{mn}\"."
            print(msg)
            logger.info(msg)
            exit(1)

        if verify:
            sys_meta: SysMeta = gmn_client.get_system_metadata(pid)
            print("\n")
            print(sys_meta.model_dump_json(indent=4, exclude_none=exclude_none))
        exit(0)

    # Read and display system metadata, then exit.
    try:
        sys_meta: SysMeta = gmn_client.get_system_metadata(pid)
    except d1_exceptions.NotFound as e:
        msg = f"System metadata not found for \"{pid}\" on \"{mn}\"."
        print(msg)
        logger.info(msg)
        exit(1)

    logger.info(f"Dumping system metadata for \"{mn}: {pid}\".")
    print(sys_meta.model_dump_json(indent=4, exclude_none=exclude_none))
    exit(0)
