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
from gmn_adapter.models.pasta.pasta_db import get_pasta_db_engine

logger = daiquiri.getLogger(__name__)


def test_package(config):
    pasta_db_engine = get_pasta_db_engine(host=config["db_host"], port=config["db_port"])
    package = Package(pid="cos-spu.10.1", pasta_db_engine=pasta_db_engine)
    assert package is not None

    print("\n")
    print(package)
    print(package.ore)
