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
from gmn_adapter.lock import Lock
from gmn_adapter.exceptions import GMNAdapterDataPackageExists
from gmn_adapter.models.adapter.adapter_db import QueueManager
from gmn_adapter.models.pasta.package import Package
from gmn_adapter.models.pasta.pasta_db import get_pasta_db_engine
from gmn_adapter.cli.syncing.synchronize import synchronize_to_gmn


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


def sync_manager(lock_file: str, verbose: int) -> int:
    """Manage synchronization tasks to the GMN."""

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
    queue_head = queue_manager.get_head(clean=True)
    while queue_head:
        pid = str(queue_head.package)
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

        queue_head = queue_manager.get_head(clean=True)

    lock.release()
    logger.warning('Lock file {} released'.format(lock.lock_file))

    return 0


def print_version(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    print(f"Version: {Config.VERSION.read_text("utf-8")}")
    ctx.exit()


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
help_conf = "Current configuration settings."
help_lock = "Path to lock file (default /tmp/sync_manager.lock)."
help_verbose = "Send output to standard out (-v or -vv or -vvv for increasing output)."
help_version = "Output GMN adapter version and exit."

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--conf", is_flag=True, default=False, help=help_conf)
@click.option("-l", "--lock", type=str, default="/tmp/sync_manager.lock", help=help_lock)
@click.option("-v", "--verbose", count=True, help=help_verbose)
@click.option("--version", is_flag=True, default=False, callback=print_version, expose_value=False, is_eager=True, help=help_version)
def cli(conf: bool, lock: str, verbose: int):
    """CLI wrapper for the sync_manager function.\n

    The sync_manager function manages synchronization of data packages to the GMN based on the
    entries of the adapter queue database.

    """
    if conf:
        click.echo(json.dumps(configuration(), indent=4))
        sys.exit(0)
    else:
        status= sync_manager(lock_file=lock, verbose=verbose)
        sys.exit(status)


if __name__ == "__main__":
    cli()
