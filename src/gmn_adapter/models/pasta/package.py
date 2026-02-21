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

from sqlalchemy import Engine

from gmn_adapter.exceptions import (
    GMNAdapterDataPackageNotFound, GMNAdapterDataPackageResourcesNotFound, GMNAdapterPackageIsNotGMNCandidate
)
import gmn_adapter.iam.client as iam_client
import gmn_adapter.models.dataone.ore as ore
from gmn_adapter.models.pasta.resource_registry import ResourceRegistry
from gmn_adapter.models.pasta.resource_map import ResourceMap
from gmn_adapter.models.pasta.resource_type import ResourceType


logger = daiquiri.getLogger(__name__)


def _get_package_resources(resource_registry: ResourceRegistry, scope: str, identifier: str, revision: str) -> list[tuple]:
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
        r = (
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
        )
        resource_list.append(r)

    return resource_list


def _get_resource_types(resources: list) -> dict:
    resource_types = {}
    data_entities = []
    for resource in resources:
        resource_type = ResourceType(resource[0])
        resource_id = resource[1]
        if resource_type == ResourceType.DATA:
            data_entities.append(resource_id)
        else:
            resource_types[resource_type] = resource_id
    resource_types[ResourceType.DATA] = data_entities

    return resource_types


def _get_package_doi(resources: list) -> str | None:
    return [r[ResourceMap.DOI] for r in resources if r[ResourceMap.RESOURCE_TYPE] == ResourceType.DATA_PACKAGE][0]



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
            self._resources: list[tuple] = _get_package_resources(resource_registry, self._scope, self._identifier, self._revision)
        except GMNAdapterDataPackageResourcesNotFound as e:
            msg = f"Data package \"{pid}\" was not found on PASTA."
            raise GMNAdapterDataPackageNotFound(msg) from e
        self._resource_types = _get_resource_types(self._resources)
        self._doi = _get_package_doi(self._resources)
        self._date_deactivated = resource_registry.get_date_deactivated(self._scope, self._identifier, self._revision)
        if self._is_gmn_candidate():
            # Generate ORE document for the data package and add to resources
            self._ore: bytes = ore.get_ore(resources=self._resources)
            resource = (
                ResourceType.ORE,
                self._doi,
                None,
                f"{self._pid}-ore.xml",
                datetime.now(timezone.utc).isoformat(),
                hashlib.sha1(self._ore).hexdigest(),
                hashlib.md5(self._ore).hexdigest(),
                "http://www.openarchives.org/ore/terms",
                "application/xml",
                len(self._ore)
            )
            self._resources.append(resource)
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
    def resources(self) -> list:
        return self._resources

    @property
    def resource_types(self) -> dict:
        return self._resource_types

    @property
    def doi(self) -> str:
        return self._doi

    @property
    def date_deactivated(self) -> datetime | None:
        return self._date_deactivated

    @property
    def ore(self) -> str:
        return self._ore.decode("utf-8")

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
        for resource_type, resource_id in self._resource_types.items():
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


