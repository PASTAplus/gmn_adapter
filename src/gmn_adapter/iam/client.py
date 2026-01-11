#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary:

Module:
    client

Author:
    servilla

Created:
    2026-01-11
"""
import daiquiri

from iam_lib.api.authorized import AuthorizedClient
from iam_lib.api.edi_token import EdiTokenClient
from iam_lib.exceptions import IAMResponseError

from gmn_adapter.config import Config

logger = daiquiri.getLogger(__name__)


def _edi_token_client(token: str = None) -> EdiTokenClient:
    return EdiTokenClient(
        scheme=Config.SCHEME,
        host=f"{Config.AUTH_HOST}:{Config.AUTH_PORT}",
        accept=Config.ACCEPT,
        public_key_path=Config.PUBLIC_KEY_PATH,
        algorithm=Config.JWT_ALGORITHM,
        token=token,
        truststore=Config.TRUSTSTORE,
        timeout=Config.CONNECT_TIMEOUT
    )


def _authorized_client(token: str = None) -> AuthorizedClient:
    return AuthorizedClient(
        scheme=Config.SCHEME,
        host=f"{Config.AUTH_HOST}:{Config.AUTH_PORT}",
        accept=Config.ACCEPT,
        public_key_path=Config.PUBLIC_KEY_PATH,
        algorithm=Config.JWT_ALGORITHM,
        token=token,
        truststore=Config.TRUSTSTORE,
        timeout=Config.CONNECT_TIMEOUT
    )


def get_public_token() -> str:
    try:
        response = _edi_token_client().create_token(Config.PUBLIC_ID, Config.AUTH_KEY)
        return response["edi-token"]
    except IAMResponseError as e:
        msg = f"Failed to create public token: {e}"
        raise IAMResponseError(msg) from e

def is_authorized(token: str, resource: str, permission: str) -> bool:
    try:
        return _authorized_client(token).is_authorized(resource, permission)
    except IAMResponseError as e:
        msg = f"Failed to assess privilege for resource {resource} and permission {permission}"
        raise IAMResponseError(msg) from e