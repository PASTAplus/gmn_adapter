#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: A Python representation of the DataONE system metadata.

Module:
    sysmeta

Author:
    servilla

Created:
    2025-12-31
"""
from dataclasses import dataclass, asdict
import json
from datetime import datetime

import daiquiri


logger = daiquiri.getLogger(__name__)

@dataclass
class SysMeta:
    serial_version: int
    identifier: str
    format_id: str
    size: int
    checksum: str
    submitter: str
    rights_holder: str
    access_policy: list
    replication_policy: list
    obsoletes: str
    obsoleted_by: str
    archived: bool
    date_uploaded: datetime
    origin_member_node: str
    authoritative_member_node: str
    replica: list

