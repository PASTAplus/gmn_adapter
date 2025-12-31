#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:

Module:
    replication_policy

Author:
    servilla

Created:
    2025-12-31
"""
from dataclasses import dataclass, asdict
import json

import daiquiri


logger = daiquiri.getLogger(__name__)


@dataclass
class ReplicationPolicy:
    preferred_member_node: list
    blocked_member_node: list
    replication_allowed: bool
    number_replicas: int