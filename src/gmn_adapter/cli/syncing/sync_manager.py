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
import json
import logging
from pathlib import Path
import sys

import click
import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.cli.configuration import configuration
from gmn_adapter.cli.synchronize import synchronize_to_gmn
from gmn_adapter.exceptions import GMNAdapterDataPackageExists, GMNAdapterPartialDataPackageExists, \
    GMNAdapterNonSynchronizedAncestor, GMNAdapterPackageIsNotGMNCandidate, GMNAdapterError, \
    GMNAdapterDataPackageNotFound
from gmn_adapter.lock import Lock
from gmn_adapter.models.adapter.adapter_db import QueueManager
from gmn_adapter.models.pasta.package import Package
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


def sync_manager(dryrun: bool, repair: bool, verbose: int) -> int:
    """Manage synchronization tasks to the GMN."""

    lock = Lock(Config.SYNC_LOCK)
    if lock.locked:
        logger.error(f"Lock file {lock.lock_file} exists, exiting...")
        sys.exit(1)
    else:
        lock.acquire()
        logger.warning(f"Lock file {lock.lock_file} acquired")

    pasta_db_engine = get_pasta_db_engine()
    queue_manager = QueueManager(Config.QUEUE)
    queue_manager.set_all_clean()  # Mark all queued packages as clean for GMN synchronization inspection.
    queue_head = queue_manager.get_head(clean=True)

    while queue_head:

        # Allow for controlled exit
        if Config.SYNC_IRQ.exists():
            click.echo("Sync IRQ detected, exiting...")
            logger.info("Sync IRQ detected, exiting...")
            break

        pid = str(queue_head.package)
        if verbose > 0:
            click.echo(f"Processing package: {pid}")
        logger.info(f"Processing package: {pid}")
        try:
            package = Package(pid=pid, pasta_db_engine=pasta_db_engine)
        except GMNAdapterDataPackageNotFound:
            if verbose > 0:
                click.echo(f"Data package \"{pid}\" was not found on PASTA - skipping.")
            logger.warning(f"Data package \"{pid}\" was not found on PASTA - skipping..")
        except GMNAdapterPackageIsNotGMNCandidate as e:
            if verbose > 0:
                click.echo(f"Package \"{pid}\" is not a GMN candidate - skipping.")
            logger.warning(f"Package \"{pid}\" is not a GMN candidate - skipping.")
            queue_manager.set_dirty(package=pid)
        else:
            if verbose > 0:
                click.echo(f"Attempting to synchronize package: {package.pid}")
            logger.info(f"Attempting to synchronize package: {package.pid}")
            try:
                synchronize_to_gmn(
                    package=package,
                    queue_manager=queue_manager,
                    pasta_db_engine=pasta_db_engine,
                    repair=repair,
                    dryrun=dryrun,
                    verbose=verbose
                )
            except GMNAdapterPartialDataPackageExists as e:
                if verbose > 0:
                    click.echo(f"Missing data package resources in GMN for \"{pid}\":")
                    for resource in e.missing_resources: click.echo(f"\t{resource}")
                logger.error(f"Missing data package resources in GMN for \"{pid}\": {e}")
                queue_manager.set_dirty(package=pid)
            except GMNAdapterDataPackageExists as e:
                # Log the message and continue the loop
                if verbose > 0:
                    click.echo(f"Package \"{pid}\" already exists in GMN.")
                logger.info(f"Package \"{pid}\" already exists in GMN.")
                if not dryrun:
                    queue_manager.dequeue(package.pid)
                queue_manager.set_dirty(package=pid)
            except GMNAdapterNonSynchronizedAncestor as e:
                if verbose > 0:
                    click.echo(f"Error synchronizing package \"{pid}\"")
                logger.error(f"Error synchronizing package \"{pid}\": {e}")
                break  # An exceptional Exception has occurred
            except GMNAdapterError as e:
                if verbose > 0:
                    click.echo(f"Error synchronizing package \"{pid}\"")
                logger.error(f"Error synchronizing package \"{pid}\": {e}")
                break  # An exceptional Exception has occurred
            else:
                if verbose > 0:
                    click.echo(f"Package \"{pid}\" successfully synchronized to GMN.")
                logger.info(f"Package \"{pid}\" successfully synchronized to GMN.")
                if not dryrun:
                    queue_manager.dequeue(package.pid)
        queue_head = queue_manager.get_head(clean=True)

    lock.release()
    logger.warning(f"Lock file {lock.lock_file} released")

    sys.exit(0)


def print_version(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo(f"Version: {Config.VERSION.read_text("utf-8")}")
    ctx.exit()


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
help_conf = "Current configuration settings."
help_dryrun = "Perform dryrun synchronization of data packages to the GMN."
help_repair = "Attempt to repair an incomplete data package in the GMN."
help_verbose = "Send output to standard out (-v or -vv or -vvv for increasing output)."
help_version = "Output GMN adapter version and exit."

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--conf", is_flag=True, default=False, help=help_conf)
@click.option("--dryrun", is_flag=True, default=False, help=help_dryrun)
@click.option("--repair", is_flag=True, default=False, help=help_repair)
@click.option("-v", "--verbose", count=True, help=help_verbose)
@click.option("--version", is_flag=True, default=False, callback=print_version, expose_value=False, is_eager=True, help=help_version)
def cli(conf: bool, dryrun: bool, repair: bool, verbose: int):
    """CLI wrapper for the sync_manager function.\n

    The sync_manager function manages synchronization of data packages to the GMN based on the
    entries of the adapter queue database.

    """
    if conf:
        click.echo(json.dumps(configuration(), indent=4))
        sys.exit(0)
    else:
        status= sync_manager(dryrun=dryrun, repair=repair, verbose=verbose)
        sys.exit(status)


if __name__ == "__main__":
    cli()
