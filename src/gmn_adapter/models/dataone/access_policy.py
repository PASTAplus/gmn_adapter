#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:

Module:
    access_policy

Author:
    servilla

Created:
    2025-12-31
"""
from dataclasses import dataclass, asdict
from enum import Enum
import json

import daiquiri


logger = daiquiri.getLogger(__name__)


class Permission(Enum):
    READ = "read"
    WRITE = "write"
    CHANGE_PERMISSION = "changePermission"


@dataclass
class AccessPolicy:
    subject: str
    permission: Permission