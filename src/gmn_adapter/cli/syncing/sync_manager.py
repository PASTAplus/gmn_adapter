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
from gmn_adapter.lock import Lock
from gmn_adapter.exceptions import GMNAdapterDataPackageExists
from gmn_adapter.models.adapter.adapter_db import QueueManager
from gmn_adapter.models.pasta.package import Package
from gmn_adapter.models.pasta.pasta_db import get_pasta_db_engine
from gmn_adapter.cli.syncing.synchronize import synchronize_to_gmn


# Set up daiquiri logging: INFO and higher to LOGFILE, WARNING and higher to STDERR
CWD = Path("").resolve().as_posix()
LOGFILE = CWD + "/sync_manager.log"
daiquiri.setup(
    level=logging.INFO,
    outputs=(
        daiquiri.output.Stream(sys.stderr, level=logging.ERROR),
        daiquiri.output.File(LOGFILE, level=logging.INFO),
    ),
)
logger = daiquiri.getLogger(__name__)


def sync_manager(lock_file: str, verbose: int, version: bool) -> int:
    """Manage synchronization tasks to the GMN."""

    if version:
        print(Config.VERSION.read_text("utf-8"))
        return 0

    lock = Lock(lock_file)
    if lock.locked:
        logger.error('Lock file {} exists, exiting...'.format(lock.lock_file))
        return 1
    else:
        lock.acquire()
        logger.warning('Lock file {} acquired'.format(lock.lock_file))

    pasta_db_engine = get_pasta_db_engine()
    queue_manager = QueueManager(Config.QUEUE)
    queue_manager.set_all_clean()  # Mark all queued packages as clean for GMN synchronization inspection.
    head = queue_manager.get_head(clean=True)
    while head:
        pid = str(head.package)
        package = Package(pid=pid, pasta_db_engine=pasta_db_engine)
        if verbose > 0:
            print(f"Synchronizing package {package.pid}")
        logger.info(f"Synchronizing package {package.pid}")
        if package.is_gmn_candidate:
            try:
                synchronize_to_gmn(package=package, queue_manager=queue_manager, pasta_db_engine=pasta_db_engine)
            except RuntimeError as e:
                if verbose > 0:
                    print(f"Error synchronizing package {package.pid}")
                logger.error(f"Error synchronizing package {package.pid}: {e}")
                queue_manager.set_dirty(package=pid)
            except GMNAdapterDataPackageExists as e:
                if verbose > 0:
                    print(f"Package {package.pid} already exists in GMN.")
                logger.info(f"Package {package.pid} already exists in GMN.")
                queue_manager.dequeue(package.pid)
            else:
                if verbose > 0:
                    print(f"Package {package.pid} successfully synchronized to GMN.")
                logger.info(f"Package {package.pid} successfully synchronized to GMN.")
                queue_manager.dequeue(package.pid)
        else:
            if verbose > 0:
                print(f"Package {package.pid} is not a GMN candidate - skipping.")
            logger.warning(f"Package {package.pid} is not a GMN candidate - skipping.")
            queue_manager.set_dirty(package=pid)

        head = queue_manager.get_head(clean=True)

    lock.release()
    logger.warning('Lock file {} released'.format(lock.lock_file))

    return 0


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
help_config = "Current configuration settings."
help_lock = "Path to lock file (default /tmp/sync_manager.lock)."
help_verbose = "Send output to standard out (-v or -vv or -vvv for increasing output)."
help_version = "Output GMN adapter version and exit."

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--configuration", is_flag=True, default=False, help=help_config)
@click.option("-l", "--lock", type=str, default="/tmp/sync_manager.lock", help=help_lock)
@click.option("-v", "--verbose", count=True, help=help_verbose)
@click.option("--version", is_flag=True, default=False, help=help_version)
def cli(configuration: bool, lock: str, verbose: int, version: bool):
    """CLI wrapper for the sync_manager function.\n

    The sync_manager function manages synchronization of data packages to the GMN based on the
    entries of the adapter queue database.

    """
    _version = Config.VERSION.read_text("utf-8")
    if version:
        click.echo(f"{__name__} version: {_version}")
        sys.exit(0)
    elif configuration:
        gmn_url = Config.GMN_EDI_BASE_URL if Config.GMN_NODE == "EDI" else Config.GMN_LTER_BASE_URL
        pasta_db = f"{Config.DB_DRIVER}://{Config.DB_USER}:@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB}"
        click.echo(f"{__name__} version: {_version}")
        click.echo(f"GMN Node: {Config.GMN_NODE}")
        click.echo(f"GMN URL: {gmn_url}")
        click.echo(f"PASTA Endpoint: {Config.PASTA_SERVICE}")
        click.echo(f"PASTA DB: {pasta_db}")
        click.echo(f"Adapter DB: {Config.QUEUE}")
        click.echo(f"Log file: {LOGFILE}")
        sys.exit(0)
    else:
        status= sync_manager(lock_file=lock, verbose=verbose, version=version)
        sys.exit(status)


if __name__ == "__main__":
    cli()
