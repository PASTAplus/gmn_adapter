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
import json
from pathlib import Path
import sys

import click
import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.cli.configuration import configuration
from gmn_adapter.lock import Lock
from gmn_adapter.models.adapter.adapter_db import QueueManager
from gmn_adapter.models.adapter.event import Event
from gmn_adapter.models.pasta.resource_registry import ResourceRegistry
from gmn_adapter.models.pasta.pasta_db import get_pasta_db_engine


# Set up daiquiri logging: INFO and higher to LOGFILE, WARNING and higher to STDERR
LOGFILE = Config.LOGS_DIR / Path(__file__).with_suffix(".log").name
daiquiri.setup(
    level=logging.INFO,
    outputs=(
        daiquiri.output.Stream(sys.stderr, level=logging.ERROR),
        daiquiri.output.File(LOGFILE, level=logging.INFO),
    ),
)
logger = daiquiri.getLogger(__name__)


def poll_manager(bootstrap: bool, limit: int, node: str, timestamp: str, verbose: int, ) -> int:
    """Poll the PASTA data package manager for new resources."""

    lock = Lock(Config.POLL_LOCK)
    if lock.locked:
        logger.error(f"Lock file {lock.lock_file} exists, exiting...")
        sys.exit(1)
    else:
        lock.acquire()
        logger.warning(f"Lock file {lock.lock_file} acquired")

    match node:
        case Config.GMN_EDI_NODE:
            scope = "EDI"
        case Config.GMN_LTER_NODE:
            scope = "LTER"
        case _:
            raise ValueError(f"Invalid node: {node}")

    if bootstrap:
        if timestamp is None:
            timestamp = Config.TIMESTAMP
        Path(Config.QUEUE).unlink(missing_ok=True)

    queue_manager = QueueManager(Config.QUEUE)

    if timestamp is None:
        newest_event = queue_manager.get_newest_event()
        timestamp = newest_event.datetime.isoformat()

    pasta_db_engine = get_pasta_db_engine()
    resource_registry = ResourceRegistry(pasta_db_engine=pasta_db_engine)
    resources = resource_registry.get_from_date_created(scope=scope, date_created=timestamp, limit=limit)
    while resources:
        for resource in resources:
            package = resource[0]
            timestamp = resource[1]
            doi = resource[2]
            owner = resource[3]
            event = Event(package=package, timestamp=timestamp, owner=owner, doi=doi)
            if verbose > 0:
                click.echo(f"Enqueueing  {event}")
            logger.info(f"Enqueueing  {event}")
            queue_manager.enqueue(event)
        newest_event = queue_manager.get_newest_event()
        timestamp = newest_event.datetime.isoformat()
        resources = resource_registry.get_from_date_created(scope=scope, date_created=timestamp, limit=limit)
    else:
        logger.info(f"No new resources created after {timestamp}.")
        if verbose > 0:
            click.echo(f"No new resources created after {timestamp}.")

    lock.release()
    logger.warning(f"Lock file {lock.lock_file} released")

    sys.exit(0)

def print_version(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo(f"Version: {Config.VERSION.read_text("utf-8")}")
    ctx.exit()


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
help_bootstrap = "Bootstrap the adapter queue database."
help_conf = "Current configuration settings."
help_limit = "Chunk limit on the number of polled resources per interation (default=100)."
help_node = "GMN member node."
help_timestamp = "ISO 8601 timestamp to start polling from."
help_verbose = "Send output to standard out (-v or -vv or -vvv for increasing output)."
help_version = "Output GMN adapter version and exit."

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--bootstrap", is_flag=True, default=False, help=help_bootstrap)
@click.option("--conf", is_flag=True, default=False, help=help_conf)
@click.option("--limit", type=int, default=100, help=help_limit)
@click.option("--node", type=str, default=Config.GMN_NODE, help=help_node)
@click.option("--timestamp",type=str, default=None, help=help_timestamp)
@click.option("-v", "--verbose", count=True, help=help_verbose)
@click.option("--version", is_flag=True, default=False, callback=print_version, expose_value=False, is_eager=True, help=help_version)
def cli(bootstrap: bool, conf: bool, limit: int, node: str, timestamp: str, verbose: int, ):
    """CLI wrapper for the poll_manager function.\n

    The poll_manager function polls the PASTA data package manager for new resources and
    enqueues them in the adapter queue database. See below for options.

    """
    if conf:
        click.echo(json.dumps(configuration(), indent=4))
        sys.exit(0)
    else:
        status = poll_manager(
            bootstrap=bootstrap,
            limit=limit,
            node=node,
            timestamp=timestamp,
            verbose=verbose,
        )
        sys.exit(status)


if __name__ == "__main__":
    cli()