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
from pydantic import BaseModel, computed_field, field_validator


logger = daiquiri.getLogger(__name__)


class Event(BaseModel):
    """An optimized container representing an adapter queue event record."""

    package: str
    timestamp: datetime
    owner: str
    doi: str | None

    @field_validator("package")
    @classmethod
    def validate_package_format(cls, package: str) -> str:
        """Ensure the package identifier follows the 'scope.identifier.revision' format."""
        parts = package.split(".")
        if len(parts) != 3:
            raise ValueError(f"Package '{package}' must be in format 'scope.identifier.revision'")
        if not parts[1].isdigit() or not parts[2].isdigit():
            raise ValueError(f"Identifier and Revision in '{package}' must be numeric")
        return package

    @computed_field
    @property
    def scope(self) -> str:
        return self.package.split(".")[0]

    @computed_field
    @property
    def identifier(self) -> int:
        return int(self.package.split(".")[1])

    @computed_field
    @property
    def revision(self) -> int:
        return int(self.package.split(".")[2])

    def __str__(self):
        return f"Package={self.package}, Timestamp={self.timestamp}, Owner={self.owner}, DOI={self.doi}"