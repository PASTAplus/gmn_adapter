#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: To poll the PASTA data package manager for new resources

Module:
    poll_manager

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
from gmn_adapter.model.event import Event
from gmn_adapter.model.resource_registry import ResourceRegistry

# Set up daiquiri logging: INFO and higher to LOGFILE, WARNING and higher to STDERR
CWD = Path(".").resolve().as_posix()
LOGFILE = CWD + "/poll_manager.log"
daiquiri.setup(
    level=logging.INFO,
    outputs=(
        daiquiri.output.Stream(sys.stderr, level=logging.WARNING),
        daiquiri.output.File(LOGFILE, level=logging.INFO),
    ),
)
logger = daiquiri.getLogger(__name__)


def poll_manager(bootstrap: bool, limit: int, scope: str, timestamp: str, verbose: int, version: bool) -> int:
    """Poll the PASTA data package manager for new resources."""

    if scope not in ["EDI", "LTER"]:
        raise ValueError(f"Invalid scope: {scope}")

    if version:
        print(Config.VERSION.read_text("utf-8"))
        return 0

    if bootstrap:
        timestamp = Config.TIMESTAMP
        Path(Config.QUEUE).unlink(missing_ok=True)

    queue_manager = QueueManager(Config.QUEUE)

    if not timestamp:
        timestamp = (queue_manager.get_last_datetime()).isoformat()

    resource_registry = ResourceRegistry()
    resources = resource_registry.get_from_date_created(timestamp, limit=limit)
    while resources:
        for resource in resources:
            package = resource[0]
            timestamp = resource[1]
            doi = resource[2]
            owner = resource[3]
            event = Event(package=package, timestamp=timestamp, owner=owner, doi=doi)
            logger.info(f"Enqueue: {event}")
            if verbose > 0:
                print(f"Package: {package}, Timestamp: {timestamp}, DOI: {doi}, Owner: {owner}")
            queue_manager.enqueue(event)
        timestamp = (queue_manager.get_last_datetime()).isoformat()
        resources = resource_registry.get_from_date_created(timestamp, limit=limit)
    else:
        logger.info(f"No new resources created after: {timestamp}.")
        if verbose > 0:
            print(f"No new resources created after: {timestamp}.")

    return 0


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
help_bootstrap = "Bootstrap the adapter queue database."
help_limit = "Chunk limit on the number of polled resources per interation (default=100)."
help_scope = "PASTA based scope to poll (EDI or LTER)."
help_timestamp = "ISO 8601 timestamp to start polling from."
help_verbose = "Send output to standard out (-v or -vv or -vvv for increasing output)."
help_version = "Output GMN adapter version and exit."

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--bootstrap", is_flag=True, default=False, help=help_bootstrap)
@click.option("--limit", type=int, default=100, help=help_limit)
@click.option("--scope", type=str, help=help_scope)
@click.option("--timestamp",type=str, help=help_timestamp)
@click.option("-v", "--verbose", count=True, help=help_verbose)
@click.option("--version", is_flag=True, default=False, help=help_version)
def cli(bootstrap: bool, limit: int, scope: str, timestamp: str, verbose: int, version: bool):
    """CLI wrapper for the poll_manager function.\n

    The poll_manager function polls the PASTA data package manager for new resources and
    enqueues them in the adapter queue database. See below for options.

    """
    return poll_manager(bootstrap, limit, scope, timestamp, verbose, version)


if __name__ == "__main__":
    cli()