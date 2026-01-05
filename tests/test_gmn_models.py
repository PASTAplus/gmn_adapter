#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:

Module:
    test_gmn_models

Author:
    servilla

Created:
    2025-12-29
"""
from datetime import datetime

from pydantic import ValidationError
import pytest
import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.models.dataone.access_policy import AccessPolicy, AccessRule, Permission
from gmn_adapter.models.dataone.checksum import Checksum
from gmn_adapter.models.dataone.replica import Replica, ReplicaStatus
from gmn_adapter.models.dataone.replication_policy import ReplicationPolicy
from gmn_adapter.models.dataone.sysmeta import SysMeta
from gmn_adapter.models.mime.mime_types import MimeType


logger = daiquiri.getLogger(__name__)


def test_mime_type():
    mt = MimeType()
    assert mt is not None
    mime_type = mt.get_mime_type("xml")
    assert mime_type == "application/xml"
    assert not mt.is_valid("eml")


def test_replica():
    replica = Replica(
        replica_member_node="urn:node:EDI",
        replication_status=ReplicaStatus.QUEUED,
        replication_verified=datetime.fromisoformat("2025-12-29 12:00:00")
    )
    assert replica is not None

    replica = Replica(
        replica_member_node="urn:node:EDI",
        replication_status="queued",
        replication_verified="2025-12-29 12:00:00"
    )
    assert replica is not None

    with pytest.raises(ValidationError):
        Replica(
            replica_member_node="urn:node:EDI",
            replication_status="bad_input",
            replication_verified="2025-12-29 12:00:00"
        )


def test_replication_policy():
    replication_policy = ReplicationPolicy(
        preferred_member_node=["urn:node:EDI"],
        blocked_member_node=["urn:node:LTER"],
        replication_allowed=True,
        number_replicas=1
    )
    assert replication_policy is not None

    with pytest.raises(ValidationError):
        ReplicationPolicy(
            preferred_member_node=["urn:node:EDI", 4],
            blocked_member_node=["urn:node:LTER"],
            replication_allowed=True,
            number_replicas=1
        )


def test_access_policy():
    # Test complete access policy
    access_rule = AccessRule(
        subject=["EDI-62b5844b7c2dcfd36c37c41b13f4b037c917a901"],
        permission=[Permission.CHANGE_PERMISSION]
    )
    assert access_rule is not None

    access_policy = AccessPolicy(
        allow=[access_rule]
    )
    assert access_policy is not None

    # Test empty access policy
    access_policy = AccessPolicy(
        allow=[]
    )
    assert access_policy is not None

    # Test invalid permission
    with pytest.raises(ValidationError):
        access_rule = AccessRule(
            subject=["EDI-62b5844b7c2dcfd36c37c41b13f4b037c917a901"],
            permission=["execute"]
        )
        assert access_rule is not None


def test_sys_meta():
    checksum = Checksum(
        checksum="1234567890abcdef1234567890abcdef12345678",
        algorithm="SHA-1"
    )

    sys_meta = SysMeta(
        serial_version=1,
        identifier="https://pasta.lternet.edu/package/metadata/eml/edi/2033/2",
        format_id="eml://ecoinformatics.org/eml-2.2.0",
        size=1000,
        checksum=checksum,
        submitter="EDI-62b5844b7c2dcfd36c37c41b13f4b037c917a901",
        rights_holder="CN=PASTA-GMN,O=EDI,ST=New Mexico,C=US",
    )
    assert sys_meta is not None

    allow = AccessRule(
        subject=["EDI-62b5844b7c2dcfd36c37c41b13f4b037c917a901"],
        permission=[Permission.CHANGE_PERMISSION]
    )
    access_policy = AccessPolicy(
        allow=[allow]
    )

    replication_policy = ReplicationPolicy(
        preferred_member_node=["urn:node:EDI"],
        blocked_member_node=["urn:node:LTER"],
        replication_allowed=True,
        number_replicas=1
    )

    replica = Replica(
        replica_member_node="urn:node:EDI",
        replication_status=ReplicaStatus.QUEUED,
        replication_verified=datetime.fromisoformat("2025-12-29 12:00:00")
    )

    sys_meta = SysMeta(
        serial_version=1,
        identifier="https://pasta.lternet.edu/package/metadata/eml/edi/2033/2",
        format_id="eml://ecoinformatics.org/eml-2.2.0",
        size=1000,
        checksum=checksum,
        submitter="EDI-62b5844b7c2dcfd36c37c41b13f4b037c917a901",
        rights_holder="CN=PASTA-GMN,O=EDI,ST=New Mexico,C=US",
        access_policy=access_policy,
        replication_policy=replication_policy,
        replica=[replica],
        file_name = "eml.xml",
        media_type = MimeType(Config.MIME_TYPES).get_mime_type("xml")
    )
    assert sys_meta is not None

    dump = sys_meta.model_dump_json(indent=4)
    print("\n")
    print(dump)
