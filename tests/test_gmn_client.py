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
import pytest

import daiquiri
import d1_common.types.exceptions

from gmn_adapter.gmn.client import Client


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
