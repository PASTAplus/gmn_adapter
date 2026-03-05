#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Display configuration settings

Module:
    configuration

Author:
    servilla

Created:
    2026-02-14
"""
import daiquiri

from gmn_adapter.config import Config


logger = daiquiri.getLogger(__name__)


def configuration() -> dict:
    gmn_url = Config.GMN_EDI_BASE_URL if Config.GMN_NODE == Config.GMN_EDI_NODE else Config.GMN_LTER_BASE_URL
    pasta_db = f"{Config.DB_DRIVER}://{Config.DB_USER}:@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB}"

    return {
        "Project": Config.NAME,
        "Version": Config.VERSION.read_text("utf-8"),
        "GMN Node": Config.GMN_NODE,
        "GMN URL": gmn_url,
        "PASTA Endpoint": Config.PASTA_SERVICE,
        "PASTA DB": pasta_db,
        "Adapter DB": Config.QUEUE,
        "IAM Endpoint": Config.AUTH_HOST,
        "Log directory": str(Config.LOGS_DIR),
        "Lock directory": str(Config.LOCK_FILE_DIR)
    }