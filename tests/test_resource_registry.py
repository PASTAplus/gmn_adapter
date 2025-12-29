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


logger = daiquiri.getLogger(__name__)


def test_get_from_date_created(resource_registry):
    """Test retrieval of data package resources created on or after a specific date."""
    resources = resource_registry.get_from_date_created("2025-01-01 12:00:00")
    assert len(resources) >= 1

    # Test with limit
    resources = resource_registry.get_from_date_created("2025-01-01 12:00:00", limit=5)
    assert len(resources) == 5