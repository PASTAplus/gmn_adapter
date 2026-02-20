#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Test module for PASTA data package resource registry class.

Module:
    test_resource_registry

Author:
    servilla

Created:
    2025-12-28
"""
import daiquiri

from gmn_adapter.models.pasta.resource_registry import ResourceRegistry


logger = daiquiri.getLogger(__name__)


def test_get_from_date_created(resource_registry):
    """
    Test retrieval of data package resources created on or after a specific date.
    """
    resources = resource_registry.get_from_date_created(scope="EDI", date_created="2025-01-01 12:00:00")
    assert len(resources) >= 1

    # Test with limit
    resources = resource_registry.get_from_date_created(scope="EDI", date_created="2025-01-01 12:00:00", limit=5)
    assert len(resources) == 5


def test_get_resource_ids(resource_registry):
    """
    Test retrieval of data package resource IDs.
    """
    resource_ids = resource_registry.get_resource_ids(scope="edi", identifier="1", revision="1")
    assert len(resource_ids) >= 1
    print("\n")
    for resource_id in resource_ids:
        print(resource_id)


def test_get_resource_metadata(resource_registry):
    """
    Test retrieval of data package resource metadata.
    """
    resources = resource_registry.get_resource_ids(scope="edi", identifier="1", revision="1")
    print("\n")
    for resource in resources:
        metadata = resource_registry.get_resource_metadata(resource[0])
        assert metadata is not None
        print(f"{resource[0]}: {metadata}")

def test_get_resources(resource_registry):
    """
    Test retrieval of data package resource metadata.
    """
    resources = resource_registry.get_resources(scope="edi", identifier="1", revision="1")
    assert len(resources) >= 1
    print("\n")
    for resource in resources:
          print(f"{resource[0]}: {resource}")