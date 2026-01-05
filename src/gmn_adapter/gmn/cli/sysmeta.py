#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:

Module:
    sysmeta

Author:
    servilla

Created:
    2025-12-29
"""
import logging
from pathlib import Path
import sys

import click
import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.gmn.client import Client
from gmn_adapter.models.dataone.sysmeta import SysMeta



# Set up daiquiri logging: INFO and higher to LOGFILE, WARNING and higher to STDERR
CWD = Path(".").resolve().as_posix()
LOGFILE = CWD + "/sysmeta.log"
daiquiri.setup(
    level=logging.INFO,
    outputs=(
        daiquiri.output.Stream(sys.stderr, level=logging.WARNING),
        daiquiri.output.File(LOGFILE, level=logging.INFO),
    ),
)
logger = daiquiri.getLogger(__name__)


def print_version(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    print(f"gmn_adapter version: {Config.VERSION.read_text("utf-8")}")
    ctx.exit()


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument("mn")
@click.argument("pid")
@click.option("-f", "--full", is_flag=True, default=False,
              help="Display full system metadata, including empty fields.")
@click.option("-u", "--update", type=click.Path(exists=True),
              help="Update system metadata from JSON file.")
@click.option("-v", "--verify", is_flag=True, default=False,
              help="Verify system metadata has been updated successfully.")
@click.option("--version", is_flag=True, default=False, callback=print_version,
              expose_value=False, is_eager=True, help="Output GMN adapter version and exit.")
def sysmeta(mn: str, pid: str, full: bool, update: str, verify: bool):
    """
    Retrieve and update system metadata for a given MN and PID.

    MN: Member Node identifier (EDI or LTER).\n
    PID: Persistent Identifier (e.g., "https://pasta.lternet.edu/package/metadata/eml/edi/1/1").
    """
    if mn not in ("EDI", "LTER"):
        sys.stderr.write(f"Invalid MN identifier: {mn}. Must be either 'EDI' or 'LTER'.")
        exit(1)

    exclude_none = True
    if full:
        exclude_none = False

    client = Client(mn)

    # Update system metadata from JSON file, then exit.
    if update:
        with open(update, "r") as f:
            sys_meta: SysMeta = SysMeta.model_validate_json(f.read())
        print(sys_meta.model_dump_json(indent=4, exclude_none=exclude_none))
        print("\n")
        click.confirm("Update system metadata?", abort=True)
        click.echo(f"Updating system metadata for \"{mn}: {pid}\".")
        logger.info(f"Updating system metadata for \"{mn}: {pid}\".")
        # TODO: client.update_system_metadata(pid, sys_meta)
        if verify:
            sys_meta: SysMeta = client.get_system_metadata(pid)
            print("\n")
            print(sys_meta.model_dump_json(indent=4, exclude_none=exclude_none))
        exit(0)

    # Read and display system metadata, then exit.
    sys_meta: SysMeta = client.get_system_metadata(pid)
    logger.info(f"Dumping system metadata for \"{mn}: {pid}\".")
    print(sys_meta.model_dump_json(indent=4, exclude_none=exclude_none))
    exit(0)


if __name__ == "__main__":
    sysmeta()
