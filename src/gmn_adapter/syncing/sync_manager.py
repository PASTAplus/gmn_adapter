#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Manage synchronization tasks to the GMN.

Module:
    sync_manager

Author:
    servilla

Created:
    2025-12-28
"""
import logging
from pathlib import Path
import sys

import click
import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.model.adapter_db import QueueManager


# Set up daiquiri logging: INFO and higher to LOGFILE, WARNING and higher to STDERR
CWD = Path(".").resolve().as_posix()
LOGFILE = CWD + "/sync_manager.log"
daiquiri.setup(
    level=logging.INFO,
    outputs=(
        daiquiri.output.Stream(sys.stderr, level=logging.WARNING),
        daiquiri.output.File(LOGFILE, level=logging.INFO),
    ),
)
logger = daiquiri.getLogger(__name__)


def sync_manager(verbose: int, version: bool) -> int:
    """Manage synchronization tasks to the GMN."""

    if version:
        print(Config.VERSION.read_text("utf-8"))
        return 0

    queue_manager = QueueManager(Config.QUEUE)
    head = queue_manager.get_head()
    while head:
        package = str(head.package)
        logger.info(f"Syncing: {package}")
        if verbose > 0:
            print(f"Syncing: {package}")
        queue_manager.dequeue(package)
        head = queue_manager.get_head()

    return 0


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
help_verbose = "Send output to standard out (-v or -vv or -vvv for increasing output)."
help_version = "Output GMN adapter version and exit."

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("-v", "--verbose", count=True, help=help_verbose)
@click.option("--version", is_flag=True, default=False, help=help_version)
def cli(verbose: int, version: bool):
    """CLI wrapper for the sync_manager function.\n

    The sync_manager function manages synchronization of data packages to the GMN based on the
    entries of the adapter queue database.

    """
    return sync_manager(verbose, version)


if __name__ == "__main__":
    cli()
