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
from gmn_adapter.models.pasta.package import ResourceType
from gmn_adapter.models.pasta.resource_registry import ResourceRegistry


logger = daiquiri.getLogger(__name__)


def system_metadata_factory(resource_id: str, resource_type: ResourceType, data: bytes=None) -> SysMeta:
    """
    Creates a system metadata object for a given resource.

    Args:
        resource_id (str): The identifier for the resource for which metadata is being created.
        resource_type (ResourceType): The type of the resource being processed.
        data (bytes, optional): Additional binary data associated with the ORE resource. Defaults to None.

    Returns:
        SysMeta: An instance of the system metadata object representing the given resource.
    """
    resource_path = resource_id.split("/")
    pass
    # return SysMeta()