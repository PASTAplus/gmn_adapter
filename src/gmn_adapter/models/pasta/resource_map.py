#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Enumeration of data package resource map types.

Module:
    resource_map

Author:
    servilla

Created:
    2026-02-20
"""
from enum import IntEnum

class ResourceMap(IntEnum):
    """Enumeration of resource map types."""
    RESOURCE_TYPE = 0
    RESOURCE_ID = 1
    DOI = 2
    FILENAME = 3
    DATE_CREATED = 4
    SHA1_CHECKSUM = 5
    MD5_CHECKSUM = 6
    FORMAT_TYPE = 7
    DATA_FORMAT = 8
    RESOURCE_SIZE = 9
    PRINCIPAL_OWNER = 10

