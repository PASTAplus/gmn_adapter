#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: GMN client tools

Module:
    client

Author:
    servilla

Created:
    2025-12-29
"""


import daiquiri

import d1_common
import d1_client.cnclient_2_0
import d1_client.mnclient_2_0

from gmn_adapter.config import Config

logger = daiquiri.getLogger(__name__)


class Client:
    def __init__(self, node: str):

        if node == "EDI":
            base_url = Config.GMN_EDI_BASE_URL
            crt = Config.GMN_EDI_CERT_PATH
            key = Config.GMN_EDI_KEY_PATH
        elif node == "LTER":
            base_url = Config.GMN_LTER_BASE_URL
            crt = Config.GMN_LTER_CERT_PATH
            key = Config.GMN_LTER_KEY_PATH
        else:
            raise ValueError(f"Invalid node: {node}")

        self.client = d1_client.mnclient_2_0.MemberNodeClient_2_0(
            base_url=base_url,
            cert_pem_path=crt,
            cert_key_path=key,
            verify_tls=Config.VERIFY_TLS
        )

    def get_system_metadata(self, pid: str):
        return self.client.getSystemMetadata(pid=pid)

    def list_objects(self):
        return self.client.listObjects()

