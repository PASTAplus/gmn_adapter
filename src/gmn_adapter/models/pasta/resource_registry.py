#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Database model to extract data package metadata from the PASTA resource_registry.

Module:
    resource_registry

Author:
    servilla

Created:
    2025-12-28
"""
from datetime import datetime
from typing import Any, Sequence

import daiquiri
from sqlalchemy import create_engine, text, Row, Engine

from gmn_adapter.config import Config

logger = daiquiri.getLogger(__name__)


class ResourceRegistry:

    def __init__(self, pasta_db_engine: Engine = None):

        self._engine = pasta_db_engine

    def get_from_date_created(self, scope: str, date_created: str, limit: int=None) -> Sequence[Row[Any]]:
        """
        Retrieve an ascending ordered list of resource registry entries created after a given date.

        Args:
            scope (str): The scope (either EDI or LTER) for which to retrieve entries.
            date_created (str): The date created threshold.
            limit (int, optional): The maximum number of entries to retrieve.

        Returns:
            Sequence[Row[Any]]: The retrieved resource registry entries.

        """
        if scope == "EDI":
            scopes = Config.EDI_SCOPES
        elif scope == "LTER":
            scopes = Config.LTER_SCOPES
        else:
            raise ValueError(f"Invalid scope: {scope}")

        columns = "package_id, date_created, doi, principal_owner"
        select = (f"SELECT {columns} FROM datapackagemanager.resource_registry "
                  f"WHERE CLAUSE AND resource_type = 'dataPackage' AND "
                  f"scope IN {scopes} "
                  f"ORDER BY date_created LIMIT;")
        sql = select.replace("CLAUSE", f"date_created > '{date_created}'")
        if limit is not None:
            sql = sql.replace("LIMIT;", f"LIMIT {limit};")
        else:
            sql = sql.replace("LIMIT;", "")

        with self._engine.connect() as conn:
            result = conn.execute(text(sql))
        return result.fetchall()

    def get_resource_ids(self, scope: str, identifier: str, revision: str) -> Sequence[Row[Any]]:
        """
        Retrieves a list of data package resources identifiers and resource types.

        Args:
            scope (str): The scope value of the data package.
            identifier: The identifier value of the data package.
            revision: The revision value of the data package.

        Returns:
            Sequence[Row[Any]]: A sequence of rows containing the resource ID and resource type.
        """
        columns = "resource_id, resource_type"
        select = (f"SELECT {columns} FROM datapackagemanager.resource_registry "
                  f"WHERE scope = '{scope}' AND "
                  f"identifier = '{identifier}' AND "
                  f"revision = '{revision}';")

        with self._engine.connect() as conn:
            result = conn.execute(text(select))
        return result.fetchall()

    def get_package_doi(self, scope: str, identifier: str, revision: str) -> Sequence[Row[Any]]:
        """
        Retrieves the Digital Object Identifier (DOI) of a data package for a specific
        scope, identifier, and revision.

        Args:
            scope (str): The scope value of the data package.
            identifier: The identifier value of the data package.
            revision: The revision value of the data package.

        Returns:
            doi (str) or None: The DOI of the matching data package.
        """
        columns = "doi"
        select = (f"SELECT {columns} FROM datapackagemanager.resource_registry "
                  f"WHERE scope = '{scope}' AND "
                  f"identifier = '{identifier}' AND "
                  f"revision = '{revision}' AND "
                  f"resource_type = 'dataPackage';")

        with self._engine.connect() as conn:
            result = conn.execute(text(select))
        return result.scalar_one_or_none()

    def get_date_deactivated(self, scope: str, identifier: str, revision: str) -> datetime | None:
        """
        Retrieves the date_deactivated (or null) of a data package for a specific scope, identifier, and revision.

        This method retrieves a list of rows containing the deactivation date of a
        data package from the database. It performs a query with the specified filter
        criteria and returns the result as a sequence of rows.

        Args:
            scope (str): The scope value of the data package.
            identifier: The identifier value of the data package.
            revision: The revision value of the data package.

        Returns:
            date_deactivated (datetime) or None: The date_deactivated that matches the provided filter criteria.
        """
        columns = "date_deactivated"
        select = (f"SELECT {columns} FROM datapackagemanager.resource_registry "
                  f"WHERE scope = '{scope}' AND "
                  f"identifier = '{identifier}' AND "
                  f"revision = '{revision}' AND "
                  f"resource_type = 'dataPackage';")

        with self._engine.connect() as conn:
            result = conn.execute(text(select))
        return result.scalar_one_or_none()

    def get_resource_metadata(self, resource_id: str) -> tuple | None:
        columns = "filename, date_created, sha1_checksum, md5_checksum, format_type, data_format, resource_size"
        select = (f"SELECT {columns} FROM datapackagemanager.resource_registry "
                  f"WHERE resource_id = '{resource_id}';")

        with self._engine.connect() as conn:
            result = conn.execute(text(select))
        return result.one_or_none()


    def get_resources(self, scope: str, identifier: str, revision: str) -> Sequence[Row[Any]]:
        """
        Retrieves a list of data package resources and their associated metadata.

        Args:
            scope (str): The scope value of the data package.
            identifier: The identifier value of the data package.
            revision: The revision value of the data package.

        Returns:
            Sequence[Row[Any]]: A sequence of rows containing the resource and its associated metadata.
        """
        columns = ("resource_type, resource_id, doi, filename, date_created, sha1_checksum, md5_checksum, "
                   "format_type, data_format, resource_size, principal_owner")
        select = (f"SELECT {columns} FROM datapackagemanager.resource_registry "
                  f"WHERE scope = '{scope}' AND "
                  f"identifier = '{identifier}' AND "
                  f"revision = '{revision}';")

        with self._engine.connect() as conn:
            result = conn.execute(text(select))
        return result.fetchall()
