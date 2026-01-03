#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:
Provides a Pydantic-based model for DataONE object replicas.

Module:
    replica

Author:
    servilla

Created:
    2026-01-02
"""
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, field_validator
import daiquiri

logger = daiquiri.getLogger(__name__)


class ReplicaStatus(Enum):
    """
    Represents the different statuses a replica can have.
    """
    QUEUED = "queued"
    REQUESTED = "requested"
    COMPLETED = "completed"
    FAILED = "failed"
    INVALIDATED = "invalidated"


class Replica(BaseModel):
    """
    Represents a replica model of a DataONE object.

    Pydantic automatically handles coercion for:
    - ReplicaStatus (from valid strings)
    - datetime (from ISO 8601 strings)
    """
    # Use ConfigDict to make the model immutable (like frozen=True)
    model_config = ConfigDict(frozen=True)

    replica_member_node: str
    replication_status: ReplicaStatus
    replication_verified: datetime
