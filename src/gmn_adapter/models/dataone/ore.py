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

logger = daiquiri.getLogger(__name__)


def get_ore(pid: str = None, resources: dict = None):
    data = [resources[Config.METADATA], resources[Config.REPORT]]
    for data_resource_id in resources[Config.DATA]:
        data.append(data_resource_id)
    ore = d1_common.resource_map.ResourceMap(base_url=Config.CN_BASE_URL)
    ore.initialize(pid=pid)
    ore.addMetadataDocument(pid=resources[Config.METADATA])
    ore.addDataDocuments(scidata_pid_list=data, scimeta_pid=resources[Config.METADATA])
    return ore.serialize(format="xml").encode("utf-8")


