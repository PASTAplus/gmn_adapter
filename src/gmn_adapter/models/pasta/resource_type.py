#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: PASTA resource type enumeration.

Module:
    resource_type

Author:
    servilla

Created:
    2026-02-19
"""
from enum import StrEnum


class ResourceType(StrEnum):
    """Enumeration of resource types."""
    DATA_PACKAGE = "dataPackage"
    METADATA = "metadata"
    REPORT = "report"
    DATA = "data"
    ORE = "ore"


