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
import daiquiri

from gmn_adapter.gmn.client import Client


logger = daiquiri.getLogger(__name__)


def test_gmn_client():
    client = Client("EDI")
    assert client is not None


def test_get_system_metadata():
    pid = "https://pasta.lternet.edu/package/metadata/eml/edi/2033/1"
    client = Client("EDI")
    sys_meta = client.get_system_metadata(pid)
    assert sys_meta is not None
    print("\n")
    print(sys_meta.model_dump_json(indent=4, exclude_none=True))


def test_list_objects():
    client = Client("EDI")
    objects = client.list_objects()
    assert objects is not None
    for info in objects.objectInfo:
        print(info.identifier.value())
