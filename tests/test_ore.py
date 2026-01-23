#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Test of construction of ORE

Module:
    test_ore

Author:
    servilla

Created:
    2026-01-23
"""
import pytest

from gmn_adapter.models.dataone.ore import get_ore
from gmn_adapter.models.pasta.package import Package
from gmn_adapter.models.pasta.pasta_db import get_pasta_db_engine


def test_ore(config):
    pasta_db_engine = get_pasta_db_engine(host=config["db_host"], port=config["db_port"])
    package = Package(pid="knb-lter-nin.1.2", pasta_db_engine=pasta_db_engine)

    ore = get_ore(pid=package.doi, resources=package.resource_ids)
    assert ore is not None
    print("\n")
    print(ore.decode("utf-8"))