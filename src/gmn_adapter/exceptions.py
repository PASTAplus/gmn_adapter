#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: GMN adapter exceptions.

Module:
    exceptions

Author:
    servilla

Created:
    2026-01-11
"""
import daiquiri


logger = daiquiri.getLogger(__name__)


class GMNAdapterError(Exception):
    pass


class GMNAdapterDataPackageResourcesNotFound(GMNAdapterError):
    pass


class GMNAdapterDataPackageNotFound(GMNAdapterError):
    pass


class GMNAdapterDataPackageExists(GMNAdapterError):
    pass

