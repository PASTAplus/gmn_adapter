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
import urllib.parse
from typing import Any, Sequence

import daiquiri
from sqlalchemy import create_engine, text, Row

from gmn_adapter.config import Config

logger = daiquiri.getLogger(__name__)


class ResourceRegistry:

    def __init__(self):

        conn_str = (f"{Config.DB_DRIVER}://"
                    f"{Config.DB_USER}:{urllib.parse.quote_plus(Config.DB_PW)}@"
                    f"{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB}")
        self._engine = create_engine(conn_str)

    def get_from_date_created(self, scope: str, date_created: str, limit: int=None) -> Sequence[Row[Any]]:
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
        columns = "resource_id, resource_type"
        select = (f"SELECT {columns} FROM datapackagemanager.resource_registry "
                  f"WHERE scope = '{scope}' AND "
                  f"identifier = '{identifier}' AND "
                  f"revision = '{revision}';")

        with self._engine.connect() as conn:
            result = conn.execute(text(select))
        return result.fetchall()

    def get_package_doi(self, scope: str, identifier: str, revision: str) -> Sequence[Row[Any]]:
        columns = "doi"
        select = (f"SELECT {columns} FROM datapackagemanager.resource_registry "
                  f"WHERE scope = '{scope}' AND "
                  f"identifier = '{identifier}' AND "
                  f"revision = '{revision}' AND "
                  f"resource_type = 'dataPackage';")

        with self._engine.connect() as conn:
            result = conn.execute(text(select))
        return result.fetchall()

    def get_date_deactivated(self, scope: str, identifier: str, revision: str) -> Sequence[Row[Any]]:
        columns = "date_deactivated"
        select = (f"SELECT {columns} FROM datapackagemanager.resource_registry "
                  f"WHERE scope = '{scope}' AND "
                  f"identifier = '{identifier}' AND "
                  f"revision = '{revision}' AND "
                  f"resource_type = 'dataPackage';")

        with self._engine.connect() as conn:
            result = conn.execute(text(select))
        return result.fetchall()

