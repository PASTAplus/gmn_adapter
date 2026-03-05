#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Provide GMN management CLI commands.

Module:
    adapter

Author:
    servilla

Created:
    2025-12-29
"""
import json
import logging
from pathlib import Path
import sys

import click
import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.cli.configuration import configuration
from gmn_adapter.cli.adapter.peekage import peekage
from gmn_adapter.cli.adapter.qstat import qstat
from gmn_adapter.cli.adapter.sysmeta import sysmeta
from gmn_adapter.cli.adapter.sync import sync


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


def print_conf(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo(json.dumps(configuration(), indent=4))
    ctx.exit()


def sync_irq(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    # Toggle sync_manager interrupt request
    if Config.SYNC_IRQ.exists():
        Config.SYNC_IRQ.unlink()
    else:
        Config.SYNC_IRQ.touch()
    ctx.exit()


def print_version(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    if not value or ctx.resilient_parsing:
        return
    click.echo(f"Version: {Config.VERSION.read_text("utf-8")}")
    ctx.exit()


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
help_conf = "Current configuration settings."
help_sync_irq = "Toggle sync_manager interrupt request."
help_version = "Output GMN adapter version and exit."


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option("--conf", is_flag=True, default=False, callback=print_conf, expose_value=False, is_eager=True, help=help_conf)
@click.option("--sync_irq", is_flag=True, default=False, callback=sync_irq, expose_value=False, is_eager=True, help=help_sync_irq)
@click.option("--version", is_flag=True, default=False, callback=print_version, expose_value=False, is_eager=True, help=help_version)
@click.pass_context
def adapter(ctx):
    """
    Provides GMN management CLI commands.
    """
    pass


adapter.add_command(peekage)
adapter.add_command(qstat)
adapter.add_command(sync)
adapter.add_command(sysmeta)


if __name__ == "__main__":
    adapter()
