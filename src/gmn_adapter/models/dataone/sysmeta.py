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
import d1_common.types.generated.dataoneTypes_v1 as dataoneTypes_v1
import d1_common.types.generated.dataoneTypes_v2_0 as dataoneTypes_v2_0
from pydantic import BaseModel, Field

from gmn_adapter.models.dataone.access_policy import AccessPolicy, AccessRule
from gmn_adapter.models.dataone.checksum import Checksum
from gmn_adapter.models.dataone.replica import Replica
from gmn_adapter.models.dataone.replication_policy import ReplicationPolicy

logger = daiquiri.getLogger(__name__)


class SysMeta(BaseModel):
    """
    Represents system metadata in the DataONE system.

    Provides a data model for system metadata used in the DataONE infrastructure.
    Includes properties such as identifier, format, size, checksum, replication
    policy, and other metadata attributes. This class can convert to and from
    DataONE system metadata pyxb objects, allowing seamless integration within the
    DataONE system.

    Attributes:
        serial_version (Optional[int]): The version of the serialization.
        identifier (str): The unique identifier of the metadata object.
        format_id (str): The format or MIME type identifier.
        size (int): The size of the object in bytes.
        checksum (Checksum): The checksum of the object to ensure integrity.
        submitter (Optional[str]): The identity of the entity that submitted the object.
        rights_holder (str): The identity of the entity holding the rights to the object.
        access_policy (Optional[AccessPolicy]): An access control policy for the object.
        replication_policy (Optional[ReplicationPolicy]): A policy defining replication behavior.
        replica (Optional[List[Replica]]): List of replicas of the object.
        obsoletes (Optional[str]): The identifier of the object that this version obsoletes.
        obsoleted_by (Optional[str]): The identifier of the object that obsoletes this version.
        archived (Optional[bool]): A flag indicating if the object is archived.
        date_uploaded (Optional[datetime]): The datetime the object was uploaded.
        origin_member_node (Optional[str]): The node that originally uploaded the object.
        authoritative_member_node (Optional[str]): The authoritative node for the object.
        series_id (Optional[str]): A series identifier for the object.
        media_type (Optional[str]): The media type associated with the object.
        file_name (Optional[str]): The original file name of the object.
    """

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
    date_sys_metadata_modified: Optional[datetime] = None
    origin_member_node: Optional[str] = None
    authoritative_member_node: Optional[str] = None
    series_id: Optional[str] = None
    media_type: Optional[str] = None
    file_name: Optional[str] = None

    @classmethod
    def from_pyxb(cls, system_metadata: SystemMetadata) -> SysMeta:
        """
        Create a SysMeta model instance from a DataONE system metadata pyxb object. See
        here for details: https://dataoneorg.github.io/api-documentation/apis/Types.html#.

        Args:
            system_metadata (SystemMetadata): The DataONE system metadata pyxb object to convert.
        """

        serial_version = None
        if system_metadata.serialVersion is not None:
            serial_version = system_metadata.serialVersion

        checksum = Checksum(
            checksum=system_metadata.checksum.value(),
            algorithm=system_metadata.checksum.algorithm
        )

        submitter = None
        if system_metadata.submitter is not None:
            submitter = system_metadata.submitter.value()

        access_policy = None
        if system_metadata.accessPolicy is not None:
            access_rules = []
            for access_rule in system_metadata.accessPolicy.allow:
                subjects = []
                permissions = []
                for subject in access_rule.subject:
                    subjects.append(subject.value())
                for permission in access_rule.permission:
                    permissions.append(permission)
                access_rules.append(AccessRule(subject=subjects, permission=permissions))
            access_policy = AccessPolicy(allow=access_rules)

        replication_policy = None
        if system_metadata.replicationPolicy is not None:
            preferred_member_node = []
            blocked_member_node = []
            for node in system_metadata.replicationPolicy.preferredMemberNode:
                preferred_member_node.append(node.value())
            for node in system_metadata.replicationPolicy.blockedMemberNode:
                blocked_member_node.append(node.value())
            replication_policy = ReplicationPolicy(
                preferred_member_node=preferred_member_node,
                blocked_member_node=blocked_member_node,
                replication_allowed=system_metadata.replicationPolicy.replicationAllowed,
                number_replicas=system_metadata.replicationPolicy.numberReplicas
            )

        replica = None
        if system_metadata.replica is not None:
            replicas = []
            for replica in system_metadata.replica:
                replicas.append(Replica(
                    replica_member_node=replica.replicaMemberNode.value(),
                    replication_status=replica.replicationStatus.value(),
                    replication_verified=datetime.fromisoformat(replica.replicationVerified.value())
                 ))

        obsoletes = None
        if system_metadata.obsoletes is not None:
            obsoletes = system_metadata.obsoletes.value()

        obsoleted_by = None
        if system_metadata.obsoletedBy is not None:
            obsoleted_by = system_metadata.obsoletedBy.value()

        archived = None
        if system_metadata.archived is not None:
            archived = system_metadata.archived

        date_uploaded = None
        if system_metadata.dateUploaded is not None:
            date_uploaded = system_metadata.dateUploaded

        date_sys_metadata_modified = None
        if system_metadata.dateSysMetadataModified is not None:
            date_sys_metadata_modified = system_metadata.dateSysMetadataModified

        origin_member_node = None
        if system_metadata.originMemberNode is not None:
            origin_member_node = system_metadata.originMemberNode.value()

        authoritative_member_node = None
        if system_metadata.authoritativeMemberNode is not None:
            authoritative_member_node = system_metadata.authoritativeMemberNode.value()

        series_id = None
        if system_metadata.seriesId is not None:
            series_id = system_metadata.seriesId.value()

        media_type = None
        if system_metadata.mediaType is not None:
            media_type = system_metadata.mediaType.value()

        file_name = None
        if system_metadata.fileName is not None:
            file_name = system_metadata.fileName

        return cls(
            serial_version=serial_version,
            identifier=system_metadata.identifier.value(),
            format_id=system_metadata.formatId,
            size=system_metadata.size,
            checksum=checksum,
            submitter=submitter,
            rights_holder=system_metadata.rightsHolder.value(),
            access_policy=access_policy,
            replication_policy=replication_policy,
            replica=replica,
            obsoletes=obsoletes,
            obsoleted_by=obsoleted_by,
            archived=archived,
            date_uploaded=date_uploaded,
            date_sys_metadata_modified=date_sys_metadata_modified,
            origin_member_node=origin_member_node,
            authoritative_member_node=authoritative_member_node,
            series_id=series_id,
            media_type=media_type,
            file_name=file_name
        )

    @classmethod
    def to_pyxb(cls, sys_meta: SysMeta) -> SystemMetadata:
        """
        Create a DataONE system metadata pyxb object from a SysMeta model instance. See
        here for details: https://dataoneorg.github.io/api-documentation/apis/Types.html#.

        Args:
            sys_meta (SysMeta): The DataONE SysMeta model instance to convert.
        """
        system_metadata = dataoneTypes_v2_0.systemMetadata()

        if sys_meta.serial_version is not None:
            logger.warning("Serial version is set by the member node when creating a new object - ignoring.")

        system_metadata.identifier = sys_meta.identifier
        system_metadata.formatId = sys_meta.format_id
        system_metadata.size = sys_meta.size
        system_metadata.checksum = sys_meta.checksum.checksum
        system_metadata.checksum.algorithm = sys_meta.checksum.algorithm

        if sys_meta.submitter is not None:
            logger.warning("Submitter is set by the member node when creating a new object - ignoring.")

        system_metadata.rightsHolder = sys_meta.rights_holder

        if sys_meta.access_policy is not None:
            access_policy = dataoneTypes_v1.accessPolicy()
            for access_rule in sys_meta.access_policy.allow:
                access_rule_d1 = dataoneTypes_v1.accessRule()
                for subject in access_rule.subject:
                    access_rule_d1.subject.append(subject)
                for permission in access_rule.permission:
                    access_rule_d1.permission.append(permission.value)
                access_policy.allow.append(access_rule_d1)
            system_metadata.accessPolicy = access_policy

        if sys_meta.replication_policy is not None:
            replication_policy = dataoneTypes_v1.replicationPolicy()
            replication_policy.preferredMemberNode.extend(sys_meta.replication_policy.preferred_member_node)
            replication_policy.blockedMemberNode.extend(sys_meta.replication_policy.blocked_member_node)
            replication_policy.replicationAllowed = sys_meta.replication_policy.replication_allowed
            replication_policy.numberReplicas = sys_meta.replication_policy.number_replicas
            system_metadata.replicationPolicy = replication_policy

        if sys_meta.replica is not None:
            for replica in sys_meta.replica:
                replica_d1 = dataoneTypes_v1.replica()
                replica_d1.replicaMemberNode = replica.replica_member_node
                replica_d1.replicationStatus = replica.replication_status.value
                replica_d1.replicationVerified = replica.replication_verified.isoformat()
                system_metadata.replica.append(replica_d1)

        if sys_meta.obsoletes is not None:
            system_metadata.obsoletes = sys_meta.obsoletes

        if sys_meta.obsoleted_by is not None:
            system_metadata.obsoletedBy = sys_meta.obsoleted_by

        if sys_meta.archived is not None:
            system_metadata.archived = sys_meta.archived

        if sys_meta.date_uploaded is not None:
            system_metadata.dateUploaded = sys_meta.date_uploaded.isoformat()

        if sys_meta.date_sys_metadata_modified is not None:
            system_metadata.dateSysMetadataModified = sys_meta.date_sys_metadata_modified.isoformat()

        if sys_meta.origin_member_node is not None:
            system_metadata.originMemberNode = sys_meta.origin_member_node

        if sys_meta.authoritative_member_node is not None:
            system_metadata.authoritativeMemberNode = sys_meta.authoritative_member_node

        if sys_meta.series_id is not None:
            system_metadata.seriesId = sys_meta.series_id

        if sys_meta.media_type is not None:
            system_metadata.mediaType = sys_meta.media_type

        if sys_meta.file_name is not None:
            system_metadata.fileName = sys_meta.file_name

        return system_metadata
