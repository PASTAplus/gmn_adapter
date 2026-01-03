#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Load up IANA mime types from CSV files into an accessible dictionary.
   Note: I decided to load the mime types from CSV files instead of having a static dictionary for
   flexibility and ease of maintainability.

Module:
    mime_types

Author:
    servilla

Created:
    2026-01-01
"""
import csv
from pathlib import Path

import daiquiri

from gmn_adapter.config import Config


logger = daiquiri.getLogger(__name__)


class MimeType:
    def __init__(self, assets_path: str = Config.MIME_TYPES):
        """
        Initialize the MimeType class by reading IANA mime types from CSV files.

        Args:
            assets_path (str): The directory path containing the IANA CSV files.
        """
        self.mime_types = {}
        self._load_mime_types(assets_path)

    def _load_mime_types(self, assets_path: str):
        """
        Reads all CSV files in the assets directory and populates the mime_types dict.
        """
        path = Path(assets_path)
        if not path.is_dir():
            raise FileNotFoundError(f"Assets directory not found: {assets_path}")

        for csv_file in path.glob("*.csv"):
            with open(csv_file, mode="r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                next(reader)  # Skip the header row
                for row in reader:
                    if len(row) >= 2:
                        key = row[0]
                        value = row[1]
                        self.mime_types[key] = value

    def get_mime_type(self, name: str) -> str | None:
        """
        Returns the mime type for a given name key.
        """
        return self.mime_types.get(name)

    def is_valid(self, mime_type: str) -> bool:
        """
        Validates if the provided mime type is present in the loaded mime types.
        """
        return mime_type in self.mime_types
