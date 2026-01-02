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

import pytest
import daiquiri
import d1_common.types.exceptions

from gmn_adapter.gmn.client import Client
from gmn_adapter.models.dataone.replica import Replica, ReplicaStatus
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

    with pytest.raises(TypeError):
        Replica(
            replica_member_node="urn:node:EDI",
            replication_status="bad_input",
            replication_verified="2025-12-29 12:00:00"
        )
