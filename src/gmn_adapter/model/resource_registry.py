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

COLUMNS = "package_id, date_created, doi, principal_owner"
SELECT = (f"SELECT {COLUMNS} FROM datapackagemanager.resource_registry WHERE CLAUSE AND resource_type = 'dataPackage' AND "
          f"scope LIKE '{Config.GMN_SCOPE}%' ORDER BY date_created LIMIT;")

class ResourceRegistry:

    def __init__(self):
        conn_str = (f"{Config.DB_DRIVER}://"
                    f"{Config.DB_USER}:{urllib.parse.quote_plus(Config.DB_PW)}@"
                    f"{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB}")
        self._engine = create_engine(conn_str)

    def get_from_date_created(self, date_created, limit=None) -> Sequence[Row[Any]]:
        sql = SELECT.replace("CLAUSE", f"date_created >'{date_created}'")
        if limit is not None:
            sql = sql.replace("LIMIT;", f"LIMIT {limit};")
        else:
            sql = sql.replace("LIMIT;", "")

        with self._engine.connect() as conn:
            result = conn.execute(text(sql))
        return result.fetchall()