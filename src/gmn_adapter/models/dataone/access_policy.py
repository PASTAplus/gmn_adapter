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
    """
    Represents the different permissions that can be granted to subjects.
    """
    READ = "read"
    WRITE = "write"
    CHANGE_PERMISSION = "changePermission"


@dataclass
class AccessPolicy:
    subject: str
    permission: Permission

    def __post_init__(self):
        """
        Perform post-initialization validation and coercion of components from inputs.
        """
        if not isinstance(self.permission, Permission):
            try:
                # If passed a string like "read", convert it to the Enum
                val = Permission(self.permission)
                object.__setattr__(self, "permission", val)
            except ValueError:
                raise TypeError(
                    f"permission must be a Permission enum or valid string. "
                    f"Got: {self.permission!r}"
                )