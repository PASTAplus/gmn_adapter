#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Create a system metadata model for a data package resource

Module:
    system_metadata

Author:
    servilla

Created:
    2026-02-19
"""
import daiquiri
import requests

from gmn_adapter.config import Config
from gmn_adapter.exceptions import GMNAdapterError
from gmn_adapter.models.dataone.sysmeta import SysMeta
from gmn_adapter.models.dataone.access_policy import AccessPolicy, AccessRule, Permission
from gmn_adapter.models.dataone.checksum import Checksum
from gmn_adapter.models.dataone.replica import Replica
from gmn_adapter.models.dataone.replication_policy import ReplicationPolicy
from gmn_adapter.models.mime.mime_types import MimeType
from gmn_adapter.models.pasta.package import ResourceMap
from gmn_adapter.models.pasta.package import ResourceType

logger = daiquiri.getLogger(__name__)


def _get_resource(resource_id: str) -> bytes:
    r = requests.get(resource_id)
    r.raise_for_status()
    return r.content


def system_metadata_factory(package_id: str, resource: tuple) -> SysMeta:
    """
    Creates a system metadata object for a given resource.

    Args:
        package_id (str): The identifier of the data package containing the resource.
        resource (tuple): A tuple containing resource information.

    Returns:
        SysMeta: An instance of the system metadata object representing the given resource.
    """
    resource_id = resource[ResourceMap.RESOURCE_ID.value]
    resource_type = resource[ResourceMap.RESOURCE_TYPE.value]

    try:
        resource_file = _get_resource(resource_id) if resource_type == ResourceType.METADATA or resource_type == ResourceType.REPORT else None
    except requests.exceptions.RequestException as e:
        msg = f"Failed to retrieve resource {resource_id} when generating system metadata: {e}"
        logger.error(msg)
        raise GMNAdapterError(msg) from e

    format_id = None
    media_type = None
    file_name = None
    series_id = package_id.rsplit('.', 1)[0]
    match resource_type:
        case ResourceType.METADATA | ResourceType.ORE:
            format_id = resource[ResourceMap.FORMAT_TYPE.value]
            media_typ = "application/xml"
            file_name = f"{package_id}.xml"
        case ResourceType.ORE :
            format_id = resource[ResourceMap.FORMAT_TYPE.value]
            media_typ = "application/xml"
            file_name = f"{package_id}-ore.xml"
        case ResourceType.REPORT:
            format_id = media_type = "application/xml"
            file_name = f"{package_id}-report.xml"
        case ResourceType.DATA:
            extension = resource[ResourceMap.FILENAME.value].split(".")[-1] if "." in resource[ResourceMap.FILENAME.value] else None
            if extension is not None and MimeType().is_valid(extension):
                format_id = media_type = MimeType().get_mime_type(extension)
            else:
                format_id = media_type = "application/octet-stream"
            file_name = resource[ResourceMap.FILENAME.value]

    size = len(resource_file) if resource[ResourceMap.RESOURCE_SIZE.value] is None else resource[ResourceMap.RESOURCE_SIZE.value]

    checksum = Checksum(
        checksum=resource[ResourceMap.SHA1_CHECKSUM.value],
        algorithm="SHA-1"
    )

    allow = AccessRule(
        subject=[f"{resource[ResourceMap.PRINCIPAL_OWNER.value]}"],
        permission=[Permission.CHANGE_PERMISSION]
    )
    access_policy = AccessPolicy(
        allow=[allow]
    )

    sys_meta = SysMeta(
        serial_version=1,
        identifier=resource_id,
        format_id=format_id,
        size=size,
        checksum=checksum,
        submitter=f"CN={Config.GMN_NODE},DC=dataone,DC=org",
        rights_holder=Config.DEFAULT_RIGHTS_HOLDER,
        origin_member_node=Config.GMN_NODE,
        authoritative_member_node=Config.GMN_NODE,
        access_policy=access_policy,
        media_type=media_type,
        series_id=series_id,
        file_name=file_name
    )

    return sys_meta
