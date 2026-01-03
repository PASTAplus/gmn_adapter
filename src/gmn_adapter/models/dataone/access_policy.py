#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:
Provides a Pydantic-based model for DataONE access policies.

Module:
    access_policy

Author:
    servilla

Created:
    2026-01-02
"""
from enum import Enum
from typing import List

from pydantic import BaseModel, ConfigDict, Field
import daiquiri

logger = daiquiri.getLogger(__name__)


class Permission(Enum):
    """
    Represents the different permissions that can be granted to subjects.
    """
    READ = "read"
    WRITE = "write"
    CHANGE_PERMISSION = "changePermission"


class AccessPolicy(BaseModel):
    """
    Represents an access policy for a DataONE object.

    Pydantic automatically handles coercion for:
    """
    model_config = ConfigDict(frozen=True)

    allow: List[Allow] = Field(default_factory=list)


class Allow(BaseModel):
    """
    Represents an allow-sequence for a DataONE allow policy.

    Pydantic automatically handles coercion for:
    """
    model_config = ConfigDict(frozen=True)

    access_rule: List[AccessRule] = Field(min_length=1)


class AccessRule(BaseModel):
    """
    Represents an access rule for a DataONE access policy.

    Pydantic automatically coerces the 'permission' field from
    valid strings (e.g., "changePermission") into the Permission Enum.
    """
    model_config = ConfigDict(frozen=True)

    subject: List[str] = Field(min_length=1)
    permission: List[Permission] = Field(min_length=1)
