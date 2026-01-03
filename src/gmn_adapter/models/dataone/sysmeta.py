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

import daiquiri
from d1_common.types.generated.dataoneTypes_v2_0 import SystemMetadata
from pydantic import BaseModel, ConfigDict, Field

from gmn_adapter.models.dataone.access_policy import AccessPolicy, Allow
from gmn_adapter.models.dataone.checksum import Checksum
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
    checksum: Checksum
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

    @classmethod
    def from_pyxb(cls, sys_meta: SystemMetadata) -> SysMeta:
        """
        Create a SysMeta model instance from a DataONE system metadata pyxb object.
        """

        checksum = Checksum(
            checksum=sys_meta.checksum.value(),
            algorithm=sys_meta.checksum.algorithm
        )

        access_policy = None
        if sys_meta.accessPolicy is not None:
            allows = []
            for allow in sys_meta.accessPolicy.allow:
                subjects = []
                permissions = []
                for subject in allow.subject:
                    subjects.append(subject.value())
                for permission in allow.permission:
                    permissions.append(permission)
                allows.append(Allow(subject=subjects, permission=permissions))
            access_policy = AccessPolicy(allow=allows)

        replication_policy = None
        if sys_meta.replicationPolicy is not None:
            preferred_member_node = []
            blocked_member_node = []
            for node in sys_meta.replicationPolicy.preferredMemberNode:
                preferred_member_node.append(node.value())
            for node in sys_meta.replicationPolicy.blockedMemberNode:
                blocked_member_node.append(node.value())
            replication_policy = ReplicationPolicy(
                preferred_member_node=preferred_member_node,
                blocked_member_node=blocked_member_node,
                replication_allowed=sys_meta.replicationPolicy.replicationAllowed,
                number_replicas=sys_meta.replicationPolicy.numberReplicas
            )

        replica = None
        if sys_meta.replica is not None:
            replicas = []
            for replica in sys_meta.replica:
                replicas.append(Replica(
                    replica_member_node=replica.replicaMemberNode.value(),
                    replication_status=replica.replicationStatus.value(),
                    replication_verified=datetime.fromisoformat(replica.replicationVerified.value())
                 ))

        obsoletes = None
        if sys_meta.obsoletes is not None:
            obsoletes = sys_meta.obsoletes.value()

        obsoleted_by = None
        if sys_meta.obsoletedBy is not None:
            obsoleted_by = sys_meta.obsoletedBy.value()

        archived = None
        if sys_meta.archived is not None:
            archived = sys_meta.archived

        date_uploaded = None
        if sys_meta.dateUploaded is not None:
            date_uploaded = sys_meta.dateUploaded

        origin_member_node = None
        if sys_meta.originMemberNode is not None:
            origin_member_node = sys_meta.originMemberNode.value()

        authoritative_member_node = None
        if sys_meta.authoritativeMemberNode is not None:
            authoritative_member_node = sys_meta.authoritativeMemberNode.value()

        series_id = None
        if sys_meta.seriesId is not None:
            series_id = sys_meta.seriesId.value()

        media_type = None
        if sys_meta.mediaType is not None:
            media_type = sys_meta.mediaType.value()

        file_name = None
        if sys_meta.fileName is not None:
            file_name = sys_meta.fileName

        return cls(
            serial_version=sys_meta.serialVersion,
            identifier=sys_meta.identifier.value(),
            format_id=sys_meta.formatId,
            size=sys_meta.size,
            checksum=checksum,
            submitter=sys_meta.submitter.value(),
            rights_holder=sys_meta.rightsHolder.value(),
            access_policy=access_policy,
            replication_policy=replication_policy,
            replica=replica,
            obsoletes=obsoletes,
            obsoleted_by=obsoleted_by,
            archived=archived,
            date_uploaded=date_uploaded,
            origin_member_node=origin_member_node,
            authoritative_member_node=authoritative_member_node,
            series_id=series_id,
            media_type=media_type,
            file_name=file_name
        )
