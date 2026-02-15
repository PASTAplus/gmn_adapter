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
LOGFILE = Config.LOGS_DIR / f"{Path(__file__).stem}.log"
daiquiri.setup(
    level=logging.INFO,
    outputs=(
        daiquiri.output.Stream(sys.stderr, level=logging.ERROR),
        daiquiri.output.File(LOGFILE, level=logging.INFO),
    ),
)
logger = daiquiri.getLogger(__name__)


def poll_manager(
    bootstrap: bool,
    limit: int,
    lock_file: str,
    scope: str,
    timestamp: str,
    verbose: int,
) -> int:
    """Poll the PASTA data package manager for new resources."""

    lock = Lock(lock_file)
    if lock.locked:
        logger.error('Lock file {} exists, exiting...'.format(lock.lock_file))
        return 1
    else:
        lock.acquire()
        logger.warning('Lock file {} acquired'.format(lock.lock_file))

    if scope not in ["EDI", "LTER"]:
        raise ValueError(f"Invalid scope: {scope}")

    if bootstrap:
        timestamp = Config.TIMESTAMP
        Path(Config.QUEUE).unlink(missing_ok=True)

    queue_manager = QueueManager(Config.QUEUE)

    if not timestamp:
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
                print(f"Enqueueing  {event}")
            logger.info(f"Enqueueing  {event}")
            queue_manager.enqueue(event)
        newest_event = queue_manager.get_newest_event()
        timestamp = newest_event.datetime.isoformat()
        resources = resource_registry.get_from_date_created(scope=scope, date_created=timestamp, limit=limit)
    else:
        logger.info(f"No new resources created after {timestamp}.")
        if verbose > 0:
            print(f"No new resources created after {timestamp}.")

    lock.release()
    logger.warning('Lock file {} released'.format(lock.lock_file))

    return 0

def print_version(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    print(f"Version: {Config.VERSION.read_text("utf-8")}")
    ctx.exit()


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
help_bootstrap = "Bootstrap the adapter queue database."
help_conf = "Current configuration settings."
help_limit = "Chunk limit on the number of polled resources per interation (default=100)."
help_lock = "Path to lock file (default /tmp/poll_manager.lock)."
help_scope = "PASTA based scopes to poll (EDI or LTER)."
help_timestamp = "ISO 8601 timestamp to start polling from."
help_verbose = "Send output to standard out (-v or -vv or -vvv for increasing output)."
help_version = "Output GMN adapter version and exit."

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--bootstrap", is_flag=True, default=False, help=help_bootstrap)
@click.option("--conf", is_flag=True, default=False, help=help_conf)
@click.option("--limit", type=int, default=100, help=help_limit)
@click.option("-l", "--lock", type=str, default="/tmp/poll_manager.lock", help=help_lock)
@click.option("--scope", type=str, default=Config.GMN_NODE, help=help_scope)
@click.option("--timestamp",type=str, help=help_timestamp)
@click.option("-v", "--verbose", count=True, help=help_verbose)
@click.option("--version", is_flag=True, default=False, callback=print_version, expose_value=False, is_eager=True, help=help_version)
def cli(bootstrap: bool, conf: bool, limit: int, lock: str, scope: str, timestamp: str, verbose: int,):
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
            lock_file=lock,
            scope=scope,
            timestamp=timestamp,
            verbose=verbose,
        )
        sys.exit(status)


if __name__ == "__main__":
    cli()