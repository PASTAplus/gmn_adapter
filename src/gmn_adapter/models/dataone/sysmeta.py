#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: A Python representation of the DataONE system metadata.

Module:
    sysmeta

Author:
    servilla

Created:
    2026-01-02
"""
from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, ConfigDict, Field
import daiquiri

from gmn_adapter.models.dataone.access_policy import AccessPolicy
from gmn_adapter.models.dataone.replica import Replica
from gmn_adapter.models.dataone.replication_policy import ReplicationPolicy

logger = daiquiri.getLogger(__name__)


class SysMeta(BaseModel):
    """
    A Pydantic representation of DataONE system metadata.

    This model nests AccessPolicy, ReplicationPolicy, and Replica models,
    allowing for full tree validation upon instantiation.
    """
    model_config = ConfigDict(frozen=True)

    serial_version: Optional[int] = None
    identifier: str
    format_id: str
    size: int
    checksum: str
    submitter: Optional[str] = None
    rights_holder: str

    # Nested models
    access_policy: Optional[AccessPolicy] = None
    replication_policy: Optional[ReplicationPolicy] = None
    # Will accept either None or an empty list if no value specified.
    replica: Optional[List[Replica]] = Field(default_factory=list)

    obsoletes: Optional[str] = None
    obsoleted_by: Optional[str] = None
    archived: Optional[bool] = None
    date_uploaded: Optional[datetime] = None
    origin_member_node: Optional[str] = None
    authoritative_member_node: Optional[str] = None
    series_id: Optional[str] = None
    media_type: Optional[str] = None
    file_name: Optional[str] = None