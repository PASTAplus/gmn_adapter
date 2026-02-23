#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Synchronize a data package with GMN.

Module:
    sync

Author:
    servilla

Created:
    2026-02-17
"""
import sys

import click
import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.cli.synchronize import synchronize_to_gmn
from gmn_adapter.exceptions import (
    GMNAdapterDataPackageExists,
    GMNAdapterPartialDataPackageExists,
    GMNAdapterNonSynchronizedAncestor,
    GMNAdapterPackageIsNotGMNCandidate, GMNAdapterError, GMNAdapterDataPackageNotFound
)
from gmn_adapter.gmn.client import Client
from gmn_adapter.lock import Lock
from gmn_adapter.models.adapter.adapter_db import QueueManager
from gmn_adapter.models.pasta.package import Package
from gmn_adapter.models.pasta.pasta_db import get_pasta_db_engine

logger = daiquiri.getLogger(__name__)


help_dryrun = "Perform dryrun synchronization of data packages to the GMN."
help_repair = "Attempt to repair an incomplete data package in the GMN."
help_verbose = "Send output to standard out (-v or -vv or -vvv for increasing output)."


@click.command()
@click.argument("pid", type=str)
@click.option("--dryrun", is_flag=True, default=False, help=help_dryrun)
@click.option("--repair", is_flag=True, default=False, help=help_repair)
@click.option("-v", "--verbose", count=True, help=help_verbose)
@click.pass_context
def sync(ctx, dryrun: bool, pid: str, repair: bool, verbose: int):
    """
    Synchronize a PASTA data package (PID) with the system GMN MN.

    PID: PASTA data package identifier to synchronize with GMN.
    """

    lock = Lock(Config.SYNC_LOCK)
    if lock.locked:
        logger.error(f"Lock file {lock.lock_file} exists, exiting...")
        sys.exit(1)
    else:
        lock.acquire()
        logger.warning(f"Lock file {lock.lock_file} acquired")

    gmn_client = Client(node=Config.GMN_NODE)

    pasta_db_engine = get_pasta_db_engine()
    queue_manager = QueueManager(Config.QUEUE)
    try:
        package = Package(pid=pid, pasta_db_engine=pasta_db_engine)
    except GMNAdapterDataPackageNotFound:
        if verbose > 0:
            click.echo(f"Data package \"{pid}\" was not found on PASTA - skipping.")
        logger.warning(f"Data package \"{pid}\" was not found on PASTA - skipping..")
    except GMNAdapterPackageIsNotGMNCandidate as e:
        if verbose > 0:
            click.echo(f"Package {pid} is not a GMN candidate - skipping.")
        logger.warning(f"Package {pid} is not a GMN candidate - skipping.")
    else:
        try:
            synchronize_to_gmn(
                package=package,
                queue_manager=queue_manager,
                pasta_db_engine=pasta_db_engine,
                gmn_client=gmn_client,
                repair=repair,
                dryrun=dryrun,
                verbose=verbose
            )
        except GMNAdapterPartialDataPackageExists as e:
            if verbose > 0:
                click.echo(f"Missing data package resources in GMN for \"{pid}\":")
                for resource in e.missing_resources: click.echo(f"\t{resource}")
            logger.error(f"Missing data package resources in GMN for \"{pid}\": {e}")
        except GMNAdapterDataPackageExists as e:
            if verbose > 0:
                click.echo(f"Package \"{pid}\" already exists in GMN.")
            logger.info(f"Package \"{pid}\" already exists in GMN.")
        except GMNAdapterNonSynchronizedAncestor as e:
            if verbose > 0:
                click.echo(f"Error synchronizing package \"{pid}\"")
            logger.error(f"Error synchronizing package \"{pid}\": {e}")
        except GMNAdapterError as e:
            if verbose > 0:
                click.echo(f"Error synchronizing package \"{pid}\"")
            logger.error(f"Error synchronizing package \"{pid}\": {e}")
        else:
            if verbose > 0:
                click.echo(f"Package \"{pid}\" successfully synchronized to GMN.")
            logger.info(f"Package \"{pid}\" successfully synchronized to GMN.")

    lock.release()
    logger.warning(f"Lock file {lock.lock_file} released")

    sys.exit(0)

