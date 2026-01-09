#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Model a PASTA data package

Module:
    package

Author:
    servilla

Created:
    2026-01-08
"""
import daiquiri


logger = daiquiri.getLogger(__name__)


class Package:
    """
    Model a PASTA data package

    Args:
        pid (str): PASTA PID in the format scope.identifier.revision
    """

    def __init__(self, pid):
        try:
            self.scope, self.identifier, self.revision = pid.split(".")
        except ValueError as ex:
            msg = f"Invalid PID format: {pid}"
            logger.error(msg)
            raise ValueError(msg) from ex


