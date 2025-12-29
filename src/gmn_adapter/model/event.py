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
from dataclasses import dataclass, field
from datetime import datetime

import daiquiri

logger = daiquiri.getLogger(__name__)


@dataclass(frozen=True)
class Event:
    """A container representing an adapter queue event record."""

    package: str
    timestamp: datetime
    owner: str
    doi: str

    # Derived fields: these are not passed to __init__
    scope: str = field(init=False)
    identifier: int = field(init=False)
    revision: int = field(init=False)

    def __post_init__(self):
        """Perform validation and derive components from the package string."""

        # Validation for Owner
        if self.owner is None:
            raise ValueError("Owner cannot be None.")

        # Extract and validate scope, identifier, and revision
        try:
            scope, identifier, revision = self.package.split(".")
            # Use object.__setattr__ because the dataclass is frozen
            object.__setattr__(self, "scope", scope)
            object.__setattr__(self, "identifier", int(identifier))
            object.__setattr__(self, "revision", int(revision))
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Invalid package format '{self.package}': {e}")

    def __str__(self):
        return f"Package={self.package}, Timestamp={self.timestamp}, Owner={self.owner}, DOI={self.doi}"
