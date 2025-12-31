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
    QUEUED = "queued"
    REQUESTED = "requested"
    COMPLETED = "completed"
    FAILED = "failed"
    INVALIDATED = "invalidated"


@dataclass
class Replica:
    replica_member_nod: str
    replication_status: ReplicaStatus
    replication_verified: datetime