#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Summary: Display adapter queue statistics.

Module:
    qstat

Author:
    servilla

Created:
    2026-03-04
"""
import sys

import click
import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.models.adapter.adapter_db import QueueManager


logger = daiquiri.getLogger(__name__)

help_queued = "List queued events."
help_verbose = "Send output to standard out (-v or -vv or -vvv for increasing output)."


@click.command()
@click.option("--queued", is_flag=True, default=False, help=help_queued)
@click.option("-v", "--verbose", count=True, help=help_verbose)
@click.pass_context
def qstat(ctx, queued: bool, verbose: int):
    """
    Display adapter queue statistics.
    """

    queue_manager = QueueManager(Config.QUEUE)
    queued_count = queue_manager.get_queued_count()
    dequeued_count = queue_manager.get_dequeued_count()
    total_count = queued_count + dequeued_count

    click.echo(f"Queued: {queued_count} ({queued_count / total_count * 100:.2f}%)")
    click.echo(f"Dequeued: {dequeued_count} ({dequeued_count / total_count * 100:.2f}%)")
    click.echo(f"Total: {total_count}")

    if queued:
        click.echo("Queued events:")
        events = queue_manager.get_queued_events()
        for event in events:
            event_out = f"    {event.package} ({event.datetime}Z)"
            click.echo(event_out)

    sys.exit(0)