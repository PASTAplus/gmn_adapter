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
import urllib.parse

import daiquiri
from sqlalchemy import create_engine, Engine

from gmn_adapter.config import Config

logger = daiquiri.getLogger(__name__)


def get_pasta_db_engine(
    db_driver: str=Config.DB_DRIVER,
    db_user: str=Config.DB_USER,
    db_pw: str=Config.DB_PW,
    host: str=Config.DB_HOST,
    port: str=Config.DB_PORT,
    db: str=Config.DB
) -> Engine:
    """
    Creates and returns a database engine for the PASTA database.

    Args:
        db_driver (str, optional): Database driver. Defaults to Config.DB_DRIVER.
        db_user (str, optional): Database user. Defaults to Config.DB_USER.
        db_pw (str, optional): Database password. Defaults to Config.DB_PW.
        host (str, optional): Database host. Defaults to Config.DB_HOST.
        port (str, optional): Database port. Defaults to Config.DB_PORT.
        db (str, optional): Database name. Defaults to Config.DB.

    Returns:
        Engine: A SQLAlchemy Engine instance connected to the specified PASTA database.
    """
    conn_str = f"{db_driver}://{db_user}:{urllib.parse.quote_plus(db_pw)}@{host}:{port}/{db}"
    return create_engine(conn_str)
