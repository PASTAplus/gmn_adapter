#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:

Module:
    test_gmn_client

Author:
    servilla

Created:
    2025-12-29
"""
from datetime import datetime

from pydantic import ValidationError
import pytest
import daiquiri

from gmn_adapter.gmn.client import Client
from gmn_adapter.models.dataone.access_policy import AccessPolicy, Allow, AccessRule, Permission
from gmn_adapter.models.dataone.replica import Replica, ReplicaStatus
from gmn_adapter.models.dataone.replication_policy import ReplicationPolicy
from gmn_adapter.models.dataone.sysmeta import SysMeta
from gmn_adapter.models.mime.mime_types import MimeType


logger = daiquiri.getLogger(__name__)


def test_gmn_client():
    client = Client("EDI")
    assert client is not None


def test_get_system_metadata():
    pid = "https://pasta.lternet.edu/package/metadata/eml/edi/2033/2"
    client = Client("EDI")
    metadata = client.get_system_metadata(pid)
    assert metadata is not None


def test_list_objects():
    client = Client("EDI")
    objects = client.list_objects()
    assert objects is not None
    for info in objects.objectInfo:
        print(info.identifier.value())


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

    allow = Allow(
        access_rule=[access_rule]
    )
    assert allow is not None

    access_policy = AccessPolicy(
        allow=[allow]
    )
    assert access_policy is not None

    # Test empty access policy
    access_policy = AccessPolicy(
        allow=[]
    )
    assert access_policy is not None

    # Test invalid access policy
    with pytest.raises(ValidationError):
        access_rule = AccessRule(
            subject=["EDI-62b5844b7c2dcfd36c37c41b13f4b037c917a901"],
            permission=["execute"]
        )
        assert access_rule is not None

        allow = Allow(
            access_rule=[access_rule]
        )
        assert allow is not None

        access_policy = AccessPolicy(
            allow=[allow]
        )


def test_sysmeta():
    sysmeta = SysMeta(
        serial_version=1,
        identifier="https://pasta.lternet.edu/package/metadata/eml/edi/2033/2",
        format_id="eml://ecoinformatics.org/eml-2.2.0",
        size=1000,
        checksum="SHA-1:1234567890abcdef1234567890abcdef12345678",
        submitter="EDI-62b5844b7c2dcfd36c37c41b13f4b037c917a901",
        rights_holder="CN=PASTA-GMN,O=EDI,ST=New Mexico,C=US",
    )
    assert sysmeta is not None

    access_rule = AccessRule(
        subject=["EDI-62b5844b7c2dcfd36c37c41b13f4b037c917a901"],
        permission=[Permission.CHANGE_PERMISSION]
    )
    allow = Allow(
        access_rule=[access_rule]
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

    sysmeta = SysMeta(
        serial_version=1,
        identifier="https://pasta.lternet.edu/package/metadata/eml/edi/2033/2",
        format_id="eml://ecoinformatics.org/eml-2.2.0",
        size=1000,
        checksum="SHA-1:1234567890abcdef1234567890abcdef12345678",
        submitter="EDI-62b5844b7c2dcfd36c37c41b13f4b037c917a901",
        rights_holder="CN=PASTA-GMN,O=EDI,ST=New Mexico,C=US",
        access_policy=access_policy,
        replication_policy=replication_policy,
        replica=[replica]
    )
    assert sysmeta is not None
