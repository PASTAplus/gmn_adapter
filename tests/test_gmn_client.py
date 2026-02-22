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

import d1_common.types.exceptions
import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.gmn.client import Client
from gmn_adapter.models.dataone.replica import Replica, ReplicaStatus
from gmn_adapter.models.dataone.replication_policy import ReplicationPolicy
from gmn_adapter.models.dataone.sysmeta import SysMeta


logger = daiquiri.getLogger(__name__)


def test_gmn_client():
    client = Client(Config.GMN_NODE)
    assert client is not None


def test_from_pyxb():
    pid = "https://pasta.lternet.edu/package/metadata/eml/edi/2033/1"
    client = Client(Config.GMN_NODE)
    system_metadata = client.get_system_metadata(pid, raw=True)
    sys_meta = SysMeta.from_pyxb(system_metadata)
    assert sys_meta is not None
    print(sys_meta.model_dump_json(indent=4, exclude_none=True))


def test_to_pyxb():
    pid = "https://pasta.lternet.edu/package/metadata/eml/edi/2033/1"
    client = Client(Config.GMN_NODE)
    sys_meta: SysMeta = client.get_system_metadata(pid)

    replication_policy = ReplicationPolicy(
        preferred_member_node=["urn:node:EDI"],
        blocked_member_node=["urn:node:LTER"],
        replication_allowed=True,
        number_replicas=1
    )
    sys_meta.replication_policy = replication_policy

    replica = Replica(
        replica_member_node="urn:node:EDI",
        replication_status=ReplicaStatus.QUEUED,
        replication_verified=datetime.fromisoformat("2025-12-29 12:00:00")
    )
    sys_meta.replica = [replica]

    system_metadata = SysMeta.to_pyxb(sys_meta)
    assert system_metadata is not None


def test_get_system_metadata():
    pid = "https://pasta.lternet.edu/package/metadata/eml/edi/2033/1"
    client = Client(Config.GMN_NODE)
    system_metadata = client.get_system_metadata(pid)
    assert system_metadata is not None
    print("\n")
    print(system_metadata.model_dump_json(indent=4, exclude_none=True))


def test_list_objects():
    client = Client(Config.GMN_NODE)
    objects = client.list_objects()
    assert objects is not None
    for info in objects.objectInfo:
        print(info.identifier.value())


def test_describe():
    pid = "https://pasta.lternet.edu/package/metadata/eml/edi/2033/1"
    client = Client(Config.GMN_NODE)
    try:
        describe = client._describe(pid)
    except d1_common.types.exceptions.NotFound as e:
        logger.error(e)
    else:
        assert describe is not None
        print("\n")
        for key, value in describe.items():
            print(f"{key}: {value}")
