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
from datetime import datetime

import daiquiri

from sqlalchemy import Engine

from gmn_adapter.config import Config
from gmn_adapter.exceptions import GMNAdapterDataPackageNotFound, GMNAdapterDataPackageResourcesNotFound
import gmn_adapter.iam.client as iam_client
import gmn_adapter.models.dataone.ore as ore
from gmn_adapter.models.pasta.resource_registry import ResourceRegistry


logger = daiquiri.getLogger(__name__)


def _get_package_resource_ids(resource_registry: ResourceRegistry, scope: str, identifier: str, revision: str) -> dict:
    """
    Retrieve resource IDs for a PASTA data package.

    Args:
        scope (str): the scope of the package
        identifier (str): the identifier of the package
        revision (str): the revision of the package

    Returns: resource_ids (dict)

    Throws: GMNAdapterDataPackageResourcesNotFound

    """
    rids = resource_registry.get_resource_ids(scope=scope, identifier=identifier, revision=revision)
    if len(rids) == 0:
        msg = f"No data package resources for \"{scope}.{identifier}.{revision}\" were found on PASTA."
        raise GMNAdapterDataPackageResourcesNotFound(msg)

    resource_ids = {}
    data_entities = []
    # Categorize resource ids into resource types
    for rid in rids:
        resource_id = rid[0]
        resource_type = rid[1]
        if resource_type == Config.PACKAGE:
            resource_ids[Config.PACKAGE] = resource_id
        elif resource_type == Config.METADATA:
            resource_ids[Config.METADATA] = resource_id
        elif resource_type == Config.REPORT:
            resource_ids[Config.REPORT] = resource_id
        elif resource_type == Config.DATA:
            data_entities.append(resource_id)
    resource_ids[Config.DATA] = data_entities
    resource_ids[Config.ORE] = resource_registry.get_package_doi(scope=scope, identifier=identifier, revision=revision)

    return resource_ids


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

        # Verify PID format
        try:
            self._scope, self._identifier, self._revision = pid.split(".")
            if len(self._scope) == 0: raise ValueError(f"Scope cannot be empty: {pid}")
            if len(self._identifier) == 0 or not self._identifier.isdigit():
                raise ValueError(f"Identifier must be a positive integer: {pid}")
            if len(self._revision) == 0 or not self._revision.isdigit():
                raise ValueError(f"Revision must be a positive integer: {pid}")
        except ValueError as e:
            msg = f"Invalid PID format: {pid}"
            logger.error(msg)
            raise ValueError(msg) from e

        resource_registry = ResourceRegistry(pasta_db_engine=pasta_db_engine)
        self._pid = pid
        try:
            self._resource_ids = _get_package_resource_ids(resource_registry, self._scope, self._identifier, self._revision)
        except GMNAdapterDataPackageResourcesNotFound as e:
            msg = f"Data package \"{pid}\" was not found on PASTA."
            raise GMNAdapterDataPackageNotFound(msg) from e
        self._doi = self._resource_ids[Config.ORE]
        self._date_deactivated = resource_registry.get_date_deactivated(self._scope, self._identifier, self._revision)
        self._is_gmn_candidate = self._is_gmn_candidate()
        self._ore = ore.get_ore(pid=self._doi, resources=self._resource_ids)

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
    def resource_ids(self) -> dict:
        return self._resource_ids

    @property
    def doi(self) -> str:
        return self._doi

    @property
    def date_deactivated(self) -> datetime | None:
        return self._date_deactivated

    @property
    def is_gmn_candidate(self) -> bool:
        return self._is_gmn_candidate

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

        data_entities = self.resource_ids[Config.DATA]
        if len(data_entities) == 0:
            logger.warning(f"Package {self.pid} has no data entities.")
            return False

        public_token = iam_client.get_public_token()

        if not iam_client.is_authorized(public_token, self.resource_ids[Config.PACKAGE], "read"):
            logger.warning(f"Package {self.pid} does not have Public read access to package metadata.")
            return False

        if not iam_client.is_authorized(public_token, self.resource_ids[Config.METADATA], "read"):
            logger.warning(f"Package {self.pid} does not have Public read access to package metadata.")
            return False

        if not iam_client.is_authorized(public_token, self.resource_ids[Config.REPORT], "read"):
            logger.warning(f"Package {self.pid} does not have Public read access to package report.")
            return False

        for entity in data_entities:
            if not iam_client.is_authorized(public_token, entity, "read"):
                logger.warning(f"Package {self.pid} does not have Public read access to data entity {entity}.")
                return False

        return True

    def __str__(self) -> str:
        """
        String representation of the Package object.

        Returns:
            str: package details
        """
        resources = ""
        for resource_type, resource_id in self._resource_ids.items():
            resources += f"    {resource_type}: {resource_id}\n"

        package_details = (
            f"Package details:\n"
            f"  PID: {self._pid}\n"
            f"  DOI: {self._doi}\n"
            f"  Date Deactivated: {self._date_deactivated}\n"
            f"  Is GMN Candidate: {self._is_gmn_candidate}\n"
            f"  Resource IDs:\n"
            f"{resources}"
        )

        return package_details


