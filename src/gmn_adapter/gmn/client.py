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

import d1_client.mnclient_2_0
from d1_common.types.generated.dataoneTypes_v2_0 import SystemMetadata

from gmn_adapter.config import Config
from gmn_adapter.models.dataone.sysmeta import SysMeta


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

    # System metadata
    def get_system_metadata(self, pid: str, raw: bool = False) -> SysMeta | SystemMetadata:
        """
        Retrieve system metadata for a given PID.

        Args:
            pid (str): The persistent identifier (PID) of the object.
            raw (bool, optional): If True, return the raw pyxb object. Defaults to False.

        Returns:
            SysMeta | SystemMetadata: The system metadata object.
        """
        system_metadata: SystemMetadata = self.client.getSystemMetadata(pid=pid) # system_metadata is a pyxb object
        if raw:
            return system_metadata  # Pyxb object
        else:
            return SysMeta.from_pyxb(system_metadata)

    def update_system_metadata(self, pid: str, sys_meta: SysMeta):
        """
        Update system metadata for a given PID.

        Args:
            pid (str): The persistent identifier (PID) of the object.
            sys_meta (SysMeta): The system metadata object.
        """
        # Sanitize system metadata for GMN
        sys_meta.serial_version = None
        sys_meta.submitter = None

        system_metadata = SysMeta.to_pyxb(sys_meta)
        logger.debug(system_metadata.toxml("utf-8"))
        self.client.updateSystemMetadata(pid=pid, sysmeta_pyxb=system_metadata)

    # GMN member node
    def list_objects(self):
        return self.client.listObjects()
