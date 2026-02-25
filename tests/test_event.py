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

from gmn_adapter.models.adapter.event import Event


logger = daiquiri.getLogger(__name__)

PACKAGE = "edi.1.1"
SCOPE = "edi"
IDENTIFIER = 1
REVISION = 1
TIMESTAMP = datetime.fromisoformat("2016-12-01 12:55:08.778000")
OWNER = "EDI-166ebf44ac70835c7ebce152e2219ae5eab16418"
DOI = "doi:10.6073/pasta/a30d5b90676008cfb7987f31b4343a35"


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
    assert event.scope == SCOPE
    assert event.identifier == IDENTIFIER
    assert event.revision == REVISION


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
