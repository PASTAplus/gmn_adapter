#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:

Module:
    test_package

Author:
    servilla

Created:
    2026-01-11
"""
import daiquiri

from gmn_adapter.models.pasta.package import Package


logger = daiquiri.getLogger(__name__)


def test_package():
    package = Package("knb-lter-nin.1.2")
    assert package is not None

    print("\n")
    print(f"pid: {package.pid}")
    print(f"doi: {package.doi}")
    print(f"date_deactivated: {package.date_deactivated}")
    for key, value in package.resource_ids.items():
        print(f"{key}: {value}")
    print(f"is_gmn_candidate: {package.is_gmn_candidate}")

    print(package)
