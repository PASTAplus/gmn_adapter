#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:

Module:
    checksum

Author:
    servilla

Created:
    2026-01-03
"""
import daiquiri
from pydantic import BaseModel, Field

logger = daiquiri.getLogger(__name__)


class Checksum(BaseModel):
    """
    DataONE checksum model.

    Attributes:
        checksum (str): Checksum value
        algorithm (str): Checksum algorithm (default: "MD5")
    """
    checksum: str
    algorithm: str = Field(default="MD5")