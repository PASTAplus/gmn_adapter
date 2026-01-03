#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:
Provides a Pydantic-based model for DataONE replication policies.

Module:
    replication_policy

Author:
    servilla

Created:
    2026-01-02
"""
from pydantic import BaseModel, ConfigDict, Field
import daiquiri

logger = daiquiri.getLogger(__name__)


class ReplicationPolicy(BaseModel):
    """
    DataONE replication policy model.

    Attributes:
        preferred_member_node (list[str]): List of nodes preferred for replication.
        blocked_member_node (list[str]): List of nodes blocked from replication.
        replication_allowed (bool): Boolean flag for replication permission.
        number_replicas (int): Desired number of replicas.
    """
    model_config = ConfigDict(frozen=True)

    # Use Field(default_factory=list) to ensure these are always lists, even if empty
    preferred_member_node: list[str] = Field(default_factory=list)
    blocked_member_node: list[str] = Field(default_factory=list)
    replication_allowed: bool = True
    number_replicas: int = 0