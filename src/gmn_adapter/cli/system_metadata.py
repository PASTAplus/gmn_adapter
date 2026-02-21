#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Create a system metadata model for a data package resource

Module:
    system_metadata

Author:
    servilla

Created:
    2026-02-19
"""
from enum import StrEnum

import daiquiri

from gmn_adapter.models.dataone.sysmeta import SysMeta
from gmn_adapter.models.pasta.package import ResourceMap
from gmn_adapter.models.pasta.package import ResourceType


logger = daiquiri.getLogger(__name__)


def system_metadata_factory(resource: tuple) -> SysMeta:
    """
    Creates a system metadata object for a given resource.

    Args:
        resource (tuple): A tuple containing resource information.

    Returns:
        SysMeta: An instance of the system metadata object representing the given resource.
    """
