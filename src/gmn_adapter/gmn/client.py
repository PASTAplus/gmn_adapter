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
from io import BytesIO

import click
import daiquiri

import d1_client
import d1_client.mnclient_2_0
import d1_common.types.exceptions as d1_exceptions
from d1_common.types.generated.dataoneTypes_v2_0 import SystemMetadata

from gmn_adapter.config import Config
from gmn_adapter.models.dataone.sysmeta import SysMeta


logger = daiquiri.getLogger(__name__)


class Client:
    def __init__(self, node: str):

        match node:
            case "urn:node:EDI":
                base_url = Config.GMN_EDI_BASE_URL
                crt = Config.GMN_EDI_CERT_PATH
                key = Config.GMN_EDI_KEY_PATH
            case "urn:node:LTER":
                base_url = Config.GMN_LTER_BASE_URL
                crt = Config.GMN_LTER_CERT_PATH
                key = Config.GMN_LTER_KEY_PATH
            case _:
                raise ValueError(f"Invalid node: {node}")

        self.client = d1_client.mnclient_2_0.MemberNodeClient_2_0(
            base_url=base_url,
            cert_pem_path=crt,
            cert_key_path=key,
            verify_tls=Config.VERIFY_TLS
        )

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

    def object_exists(self, pid: str) -> bool:
        """
        Check if an object with the given PID exists.

        Args:
            pid (str): The persistent identifier (PID) of the object.

        Returns:
            bool: True if the object exists, False otherwise.
        """
        try:
            self._describe(pid)
        except d1_exceptions.NotFound as e:
            logger.debug(f"Object {pid} does not exist: {e}")
            exists = False
        else:
            exists = True
        return exists

    def create_object(
        self,
        pid: str,
        sys_meta: SysMeta,
        data: bytes=None,
        pass_through_url: str=None,
        repair:bool=False,
        dryrun:bool=False,
        verbose: int=0
    ) -> None:
        """
        Create a GMN object with the given PID.

        Args:
            pid (str): The persistent identifier (PID) of the object.
            sys_meta (SysMeta): The system metadata object.
            data (bytes): The data to create an object with.
            pass_through_url (str, optional): The url to the PASTA data entity. Defaults to None.
            repair (bool, optional): If True, attempt to repair the object. Defaults to False.
            dryrun (bool, optional): If True, perform a dry run without actually creating the object. Defaults to False.
            verbose (int, optional): Verbosity level for logging. Defaults to 0.
        """
        if not dryrun:
            system_metadata = SysMeta.to_pyxb(sys_meta)
            if pass_through_url is not None:
                vendor_specific_header = {'VENDOR-GMN-REMOTE-URL': pass_through_url}
            else:
                vendor_specific_header = None
            try:
                self.client.create(
                    pid=pid,
                    obj=BytesIO(data),
                    sysmeta_pyxb=system_metadata,
                    vendorSpecific=vendor_specific_header
                )
            except d1_exceptions.IdentifierNotUnique as e:
                if repair:
                    msg = f"Attempting to repair data package: Object {pid} already exists, skipping..."
                    logger.warning(msg)
                    if verbose > 0:
                        click.echo(msg)
                else:
                    raise e


    def update_object(
        self,
        predecessor_pid: str,
        pid: str,
        sys_meta: SysMeta,
        data: bytes=None,
        pass_through_url:
        str=None,
        repair:bool=False,
        dryrun:bool=False,
        verbose: int=0
    ):
        """
        Update a GMN object pid with the given new PID.

        Args:
            predecessor_pid (str): The persistent identifier (PID) of the predecessor object.
            pid (str): The persistent identifier (PID) of the object.
            sys_meta (SysMeta): The system metadata object.
            data (bytes): The data to create an object with.
            pass_through_url (str, optional): The url to the PASTA data entity. Defaults to None.
            repair (bool, optional): If True, attempt to repair the object. Defaults to False.
            dryrun (bool, optional): If True, perform a dry run without actually creating the object. Defaults to False.
            verbose (int, optional): Verbosity level for logging. Defaults to 0.
        """
        if not dryrun:
            system_metadata = SysMeta.to_pyxb(sys_meta)
            if pass_through_url is not None:
                vendor_specific_header = {'VENDOR-GMN-REMOTE-URL': pass_through_url}
            else:
                vendor_specific_header = None
            try:
                self.client.update(
                    pid=predecessor_pid,
                    newPid=pid,
                    obj=BytesIO(data),
                    sysmeta_pyxb=system_metadata,
                    vendorSpecific=vendor_specific_header
                )
            except d1_exceptions.IdentifierNotUnique as e:
                if repair:
                    msg = f"Attempting to repair data package: Object {pid} already exists, skipping..."
                    logger.warning(msg)
                    if verbose > 0:
                        click.echo(msg)
                else:
                    raise e

    def delete_object(self, pid: str):
        pass

    def list_objects(self):
        return self.client.listObjects()

    def _describe(self, pid: str):
        return self.client.describe(pid=pid)

