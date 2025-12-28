#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Provides a simple getter/setter container for events.

Module:
    event.py

Attributes:
    logger: The logger instance for this module.

Author:
    Mark Servilla

Date:
    2025-12-26
"""
from datetime import datetime

import daiquiri


logger = daiquiri.getLogger(__name__)


class Event(object):
    """A container representing an adapter queue event record. """

    def __init__(self, package: str, timestamp: datetime, owner: str, doi: str):
        """Initialize the Event object.

        Args:
            package (str): The PASTA data package identifier.
            timestamp (datetime): The timestamp of the event.
            owner (str): The owner of the package.
            doi (str): The digital object identifier for the package.
        """

        self._package = package

        # Extract scope, identifier, and revision from package identifier
        scope, identifier, revision = self._package.split(".")
        self._scope = scope
        self._identifier = int(identifier)
        self._revision = int(revision)

        self._timestamp = timestamp

        if owner is None:
            raise ValueError(f"Owner cannot be None.")
        self._owner = owner

        self._doi = doi

    def __str__(self):
        return f"Package={self._package}, Timestamp={self._timestamp}, Owner={self._owner}, DOI={self._doi}"

    def __repr__(self):
        return f"<Event(package={self._package}, timestamp={self._timestamp}, owner={self._owner}, doi={self._doi})>"


    @property
    def package(self):
        """str: The PASTA data package identifier (e.g., 'scope.id.rev')."""
        return self._package

    @property
    def scope(self):
        """str: The scope of the package."""
        return self._scope

    @property
    def identifier(self):
        """int: The identifier of the package."""
        return self._identifier

    @property
    def revision(self):
        """int: The revision of the package."""
        return self._revision

    @property
    def timestamp(self):
        """datetime: The event timestamp object."""
        return self._timestamp

    @property
    def owner(self):
        """str: The owner of the package."""
        return self._owner

    @property
    def doi(self):
        """str: The digital object identifier (DOI) of the package."""
        return self._doi

