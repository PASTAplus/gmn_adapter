#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Model a PASTA data package

Module:
    package

Author:
    servilla

Created:
    2026-01-08
"""
import hashlib
from datetime import datetime, timezone

import daiquiri
from lxml import etree
import requests
from sqlalchemy import Engine
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from gmn_adapter.exceptions import (
    GMNAdapterDataPackageNotFound, GMNAdapterDataPackageResourcesNotFound, GMNAdapterPackageIsNotGMNCandidate,
    GMNAdapterError
)
import gmn_adapter.iam.client as iam_client
import gmn_adapter.models.dataone.ore as ore
from gmn_adapter.models.pasta.resource_registry import ResourceRegistry
from gmn_adapter.models.pasta.resource_map import ResourceMap
from gmn_adapter.models.pasta.resource_type import ResourceType


logger = daiquiri.getLogger(__name__)


def _get_package_resources(resource_registry: ResourceRegistry, scope: str, identifier: str, revision: str) -> list[list]:
    """
    Retrieve resources for a PASTA data package.

    Args:
        resource_registry (ResourceRegistry): resource registry instance for PASTA
            datapackagemanager.resource_registry DB model access.
        scope (str): the scope of the package.
        identifier (str): the identifier of the package.
        revision (str): the revision of the package.

    Returns: resources (list[tuple]): a list of tuples for each resource in the package.

    Throws: GMNAdapterDataPackageResourcesNotFound

    """
    resources = resource_registry.get_resources(scope=scope, identifier=identifier, revision=revision)
    if len(resources) == 0:
        msg = f"No data package resources for \"{scope}.{identifier}.{revision}\" were found on PASTA."
        raise GMNAdapterDataPackageResourcesNotFound(msg)

    resource_list = []
    for resource in resources:
        resource_type = ResourceType(resource[ResourceMap.RESOURCE_TYPE.value])
        resource_id = resource[ResourceMap.RESOURCE_ID.value].strip()
        doi = (
            resource[ResourceMap.DOI.value].strip()
            if resource[ResourceMap.DOI.value] is not None
            else None
        )
        filename = (
            resource[ResourceMap.FILENAME.value].strip()
            if resource[ResourceMap.FILENAME.value] is not None
            else None
        )
        date_created = resource[ResourceMap.DATE_CREATED.value].isoformat()
        sha1_checksum = (
            resource[ResourceMap.SHA1_CHECKSUM.value].strip()
            if resource[ResourceMap.SHA1_CHECKSUM.value] is not None
            else None
        )
        md5_checksum = (
            resource[ResourceMap.MD5_CHECKSUM.value].strip()
            if resource[ResourceMap.MD5_CHECKSUM.value] is not None
            else None
        )
        format_type = (
            resource[ResourceMap.FORMAT_TYPE.value].strip()
            if resource[ResourceMap.FORMAT_TYPE.value] is not None
            else None
        )
        data_format = (
            resource[ResourceMap.DATA_FORMAT.value].strip()
            if resource[ResourceMap.DATA_FORMAT.value] is not None
            else None
        )
        resource_size = resource[ResourceMap.RESOURCE_SIZE.value]
        principal_owner = (
            resource[ResourceMap.PRINCIPAL_OWNER.value].strip()
            if resource[ResourceMap.PRINCIPAL_OWNER.value] is not None
            else None
        )

        r = [
            resource_type,
            resource_id,
            doi,
            filename,
            date_created,
            sha1_checksum,
            md5_checksum,
            format_type,
            data_format,
            resource_size,
            principal_owner
        ]
        resource_list.append(r)

    return resource_list


def _get_package_principal_owner(resources: list) -> str | None:
    return [r[ResourceMap.PRINCIPAL_OWNER] for r in resources if r[ResourceMap.RESOURCE_TYPE] == ResourceType.DATA_PACKAGE][0]


def _get_package_doi(resources: list) -> str | None:
    return [r[ResourceMap.DOI] for r in resources if r[ResourceMap.RESOURCE_TYPE] == ResourceType.DATA_PACKAGE][0]


def _get_resource_id(resources: list, resource_type: ResourceType) -> str | None:
    return [r[ResourceMap.RESOURCE_ID] for r in resources if r[ResourceMap.RESOURCE_TYPE] == resource_type][0]


def _set_resource_size(resources: list, resource_id: str, size: int):
    resource = [r for r in resources if r[ResourceMap.RESOURCE_ID] == resource_id][0]
    resources.remove(resource)
    resource[ResourceMap.RESOURCE_SIZE] = size
    resources.append(resource)


@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=1, max=10),
)
def _get_resource_bytes(resource_id: str) -> bytes:
    r = requests.get(resource_id, timeout=(5, 30))  # Connect and read timeout
    r.raise_for_status()
    return r.content


def _get_replication_policy(eml: bytes) -> tuple | None:
    namespace_dict = {
        "eml": "eml://ecoinformatics.org/eml-2.1.1",
        "d1v1": "http://ns.dataone.org/service/types/v1"
    }
    root = etree.fromstring(eml)
    replication_policy = root.find("additionalMetadata/metadata/d1v1:replicationPolicy", namespace_dict)
    if replication_policy is not None:
        num_replicas = replication_policy.get("numberReplicas")
        rep_allowed = replication_policy.get("replicationAllowed")
        preferred_node = replication_policy.findtext(".//preferredMemberNode")
        blocked_node = replication_policy.findtext(".//blockedMemberNode")
        return rep_allowed, num_replicas, preferred_node, blocked_node
    else:
        return None


class Package:
    """
    Model a PASTA data package

    Args:
        pid (str): PASTA PID in the format scope.identifier.revision
    """

    def __init__(self, pid: str, pasta_db_engine: Engine):
        """
        Construct relevant attributes for a PASTA data package, including
        1) package identifier
        2) package identifier parts (scope, identifier, revision)
        3) package DOI (must be present to be GMN candidate)
        4) package date deactivated (must be None to be GMN candidate)
        5) resource IDs for the package (all resources must be Public Access readable to be GMN candidate)

        Args:
            pid (str): PASTA PID in the format scope.identifier.revision
            pasta_db_engine (Engine): SQLAlchemy engine instance

        Throws: ValueError, GMNAdapterDataPackageNotFound
        """
        # Resource registry instance for accessing PASTA datapackagemanager.resource_registry DB model
        resource_registry = ResourceRegistry(pasta_db_engine=pasta_db_engine)

        self._scope, self._identifier, self._revision = pid.split(".")
        self._pid = pid

        # Retrieve resources (including resource metadata) for the data package
        try:
            self._resources: list[list] = _get_package_resources(resource_registry, self._scope, self._identifier, self._revision)
        except GMNAdapterDataPackageResourcesNotFound:
            msg = f"Data package \"{pid}\" was not found on PASTA."
            raise GMNAdapterDataPackageNotFound(msg)
        self._principal_owner = _get_package_principal_owner(self._resources)
        self._doi = _get_package_doi(self._resources)
        self._date_deactivated = resource_registry.get_date_deactivated(self._scope, self._identifier, self._revision)
        self._replication_policy = None

        # Only perform resource-specific processing if the package is a GMN candidate
        if self._is_gmn_candidate():
            # Generate ORE document for the data package and add to resources
            self._ore: bytes = ore.get_ore(resources=self._resources)
            resource = [
                ResourceType.ORE,
                self._doi,
                None,
                f"{self._pid}-ore.xml",
                datetime.now(timezone.utc).isoformat(),
                hashlib.sha1(self._ore).hexdigest(),
                hashlib.md5(self._ore).hexdigest(),
                "http://www.openarchives.org/ore/terms",
                "application/xml",
                len(self._ore),
                self._principal_owner
            ]
            self._resources.append(resource)

            # Generate metadata size and replication policy for the data package and add to resources
            resource_id = _get_resource_id(self._resources, ResourceType.METADATA)
            try:
                eml = _get_resource_bytes(resource_id=resource_id)
            except requests.exceptions.RequestException as e:
                msg = f"Failed to retrieve resource {resource_id} when generating system metadata: {e}"
                logger.error(msg)
                raise GMNAdapterError(msg) from e
            else:
                eml_size = len(eml)
                _set_resource_size(resources=self._resources, resource_id=resource_id, size=eml_size)
                self._replication_policy = _get_replication_policy(eml=eml)

            # Generate report size and add to resources
            resource_id = _get_resource_id(self._resources, ResourceType.REPORT)
            try:
                report = _get_resource_bytes(resource_id=resource_id)
            except requests.exceptions.RequestException as e:
                msg = f"Failed to retrieve resource {resource_id} when generating system metadata: {e}"
                logger.error(msg)
                raise GMNAdapterError(msg) from e
            else:
                report_size = len(report)
                _set_resource_size(resources=self._resources, resource_id=resource_id, size=report_size)
        else:
            msg = f"Package \"{pid}\" is not a GMN candidate."
            raise GMNAdapterPackageIsNotGMNCandidate(msg)

    @property
    def scope(self) -> str:
        return self._scope

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def revision(self) -> str:
        return self._revision

    @property
    def pid(self) -> str:
        return self._pid

    @property
    def replication_policy(self) -> tuple:
        return self._replication_policy

    @property
    def resources(self) -> list:
        return self._resources

    @property
    def doi(self) -> str:
        return self._doi

    @property
    def date_deactivated(self) -> datetime | None:
        return self._date_deactivated

    @property
    def ore(self) -> bytes:
        return self._ore

    def _is_gmn_candidate(self) -> bool:
        """
        Fail fast if any condition is not met to be a GMN candidate.

        Returns:
            bool: True if the package is a GMN candidate, False otherwise.
        """
        if self.doi is None:
            logger.warning(f"Package {self.pid} has no DOI.")
            return False

        if self.date_deactivated is not None:
            logger.warning(f"Package {self.pid} has been deactivated.")
            return False

        data_resources = [r for r in self._resources if r[ResourceMap.RESOURCE_TYPE.value] == ResourceType.DATA]
        if len(data_resources) == 0:
            logger.warning(f"Package {self.pid} has no data entities.")
            return False

        public_token = iam_client.get_public_token()

        resource = [r for r in self._resources if r[ResourceMap.RESOURCE_TYPE.value] == ResourceType.DATA_PACKAGE][0]
        if not iam_client.is_authorized(public_token, resource[ResourceMap.RESOURCE_ID.value], "read"):
            logger.warning(f"Package {self.pid} does not have Public read access to package metadata.")
            return False

        resource = [r for r in self._resources if r[ResourceMap.RESOURCE_TYPE.value] == ResourceType.METADATA][0]
        if not iam_client.is_authorized(public_token, resource[ResourceMap.RESOURCE_ID.value], "read"):
            logger.warning(f"Package {self.pid} does not have Public read access to package metadata.")
            return False

        resource = [r for r in self._resources if r[ResourceMap.RESOURCE_TYPE.value] == ResourceType.REPORT][0]
        if not iam_client.is_authorized(public_token, resource[ResourceMap.RESOURCE_ID.value], "read"):
            logger.warning(f"Package {self.pid} does not have Public read access to package report.")
            return False

        for resource in data_resources:
            if not iam_client.is_authorized(public_token, resource[ResourceMap.RESOURCE_ID.value], "read"):
                logger.warning(f"Package {self.pid} does not have Public read access to data entity {resource}.")
                return False

        return True

    def __str__(self) -> str:
        """
        String representation of the Package object.

        Returns:
            str: package details
        """
        resources = ""
        for resource in self._resources:
            resource_type = resource[ResourceMap.RESOURCE_TYPE]
            resource_id = resource[ResourceMap.RESOURCE_ID]
            resources += f"    {resource_type}: {resource_id}\n"

        package_details = (
            f"Package details:\n"
            f"  PID: {self._pid}\n"
            f"  DOI: {self._doi}\n"
            f"  Date Deactivated: {self._date_deactivated}\n"
            f"  Resource IDs:\n"
            f"{resources}"
        )

        return package_details


