#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Create a DataONE ORE document from a PASTA data package.

Module:
    ore

Author:
    servilla

Created:
    2026-01-23
"""
import daiquiri
import d1_common.resource_map
import d1_common.types.exceptions

from gmn_adapter.config import Config
from gmn_adapter.models.pasta.resource_map import ResourceMap
from gmn_adapter.models.pasta.resource_type import ResourceType

logger = daiquiri.getLogger(__name__)


def get_ore(resources: list[list]) -> bytes:
    """
    Generates a DataONE ORE (Object Reuse and Exchange) resource map in XML format.

    This function creates and initializes an ORE resource map object, associates
    metadata and data documents, and returns the serialized ORE document in XML format
    encoded as UTF-8.

    Args:
        resources (dict): A dictionary containing PASTA resources required to generate the ORE map.

    Returns:
        bytes: The serialized ORE map in XML format encoded as UTF-8.
    """
    data = [r[ResourceMap.RESOURCE_ID] for r in resources if r[ResourceMap.RESOURCE_TYPE] != ResourceType.DATA_PACKAGE]
    doi = [r[ResourceMap.DOI] for r in resources if r[ResourceMap.RESOURCE_TYPE] == ResourceType.DATA_PACKAGE][0]
    ore = d1_common.resource_map.ResourceMap(base_url=Config.CN_BASE_URL)
    ore.initialize(pid=doi)
    metadata = [r[ResourceMap.RESOURCE_ID] for r in resources if r[ResourceMap.RESOURCE_TYPE] == ResourceType.METADATA][0]
    ore.addMetadataDocument(pid=metadata)
    ore.addDataDocuments(scidata_pid_list=data, scimeta_pid=metadata)
    return ore.serialize(format="xml").encode("utf-8")


