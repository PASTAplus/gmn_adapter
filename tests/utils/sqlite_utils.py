#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: SQLite specific utilities.

Module:
    sqlite_utils

Author:
    servilla

Created:
    2025-12-25
"""
import daiquiri
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


logger = daiquiri.getLogger(__name__)


def sqlite_memory_to_file(memory_engine: Engine, file_db: str):
    file_engine = create_engine(f"sqlite+pysqlite:///{file_db}")
    with memory_engine.connect() as memory_conn:
        with file_engine.connect() as file_conn:
            memory_src = memory_conn.connection.dbapi_connection
            file_dst = file_conn.connection.dbapi_connection
            memory_src.backup(file_dst)