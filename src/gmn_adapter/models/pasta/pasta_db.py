#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:
    Create a postgres database connection for PASTA.

Module:
    pasta_db

Author:
    servilla

Created:
    2026-01-19
"""
import urllib

import daiquiri
from sqlalchemy import create_engine, Engine

from gmn_adapter.config import Config

logger = daiquiri.getLogger(__name__)


def get_pasta_db_engine() -> Engine:
    """
    Creates and returns a database engine for the PASTA database.

    Returns:
        Engine: A SQLAlchemy Engine instance connected to the specified PASSTA database.
    """
    conn_str = (f"{Config.DB_DRIVER}://"
                f"{Config.DB_USER}:{urllib.parse.quote_plus(Config.DB_PW)}@"
                f"{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB}")
    return create_engine(conn_str)
