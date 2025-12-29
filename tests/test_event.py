#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Test module

Module:
    test_event

Author:
    servilla

Created:
    2025-12-26
"""
from datetime import datetime

import daiquiri
import pytest

from gmn_adapter.model.event import Event


logger = daiquiri.getLogger(__name__)

PACKAGE = "knb-lter-nin.1.1"
SCOPE = "knb-lter-nin"
IDENTIFIER = 1
REVISION = 1
TIMESTAMP = datetime.fromisoformat("2025-12-26 12:34:56.2345")
OWNER = "EDI-166ebf44ac70835c7ebce152e2219ae5eab16418"
DOI = "doi:10.6073/pasta/0675d3602ff57f24838ca8d14d7f3961"


def test_event():
    """Test Event model."""
    event = Event(
        package=PACKAGE,
        timestamp=TIMESTAMP,
        owner=OWNER,
        doi=DOI,
    )

    assert event.package == PACKAGE
    assert event.timestamp == TIMESTAMP
    assert event.owner == OWNER
    assert event.doi == DOI


def test_bad_package():
    with pytest.raises(ValueError):
        Event(
            package="bad_package",
            timestamp=TIMESTAMP,
            owner=OWNER,
            doi=DOI,
        )


def test_bad_owner():
    with pytest.raises(ValueError):
        Event(
            package=PACKAGE,
            timestamp=TIMESTAMP,
            owner=None,
            doi=DOI,
        )
