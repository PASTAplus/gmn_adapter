#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:

Module:
    replica

Author:
    servilla

Created:
    2025-12-31
"""
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
import json

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


@dataclass(frozen=True)
class Replica:
    """
    Represents a replica model of a DataONE object.
    """
    replica_member_node: str
    replication_status: ReplicaStatus
    replication_verified: datetime

    def __post_init__(self):
        """
        Perform post-initialization validation and coercion of components from inputs.
        """
        if not isinstance(self.replication_status, ReplicaStatus):
            try:
                # If passed a string like "queued", convert it to the Enum
                val = ReplicaStatus(self.replication_status)
                object.__setattr__(self, "replication_status", val)
            except ValueError:
                raise TypeError(
                    f"replication_status must be a ReplicaStatus enum or valid string. "
                    f"Got: {self.replication_status!r}"
                )

        if not isinstance(self.replication_verified, datetime):
            try:
                # If passed an ISO string, try to parse it
                val = datetime.fromisoformat(str(self.replication_verified))
                object.__setattr__(self, "replication_verified", val)
            except ValueError:
                raise TypeError(
                    f"replication_verified must be a datetime object or ISO string. "
                    f"Got: {self.replication_verified!r}"
                )