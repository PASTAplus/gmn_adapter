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

from gmn_adapter.models.dataone.access_policy import AccessPolicy
from gmn_adapter.models.dataone.replica import Replica
from gmn_adapter.models.dataone.replication_policy import ReplicationPolicy


logger = daiquiri.getLogger(__name__)

@dataclass
class SysMeta:
    serial_version: int | None
    identifier: str
    format_id: str
    size: int
    checksum: str
    submitter: str | None
    rights_holder: str
    access_policy: AccessPolicy | None
    replication_policy: ReplicationPolicy | None
    obsoletes: str | None
    obsoleted_by: str | None
    archived: bool | None
    date_uploaded: datetime | None
    origin_member_node: str | None
    authoritative_member_node: str | None
    replica: Replica | None
    series_id: str | None
    media_type: str | None
    file_name: str | None

