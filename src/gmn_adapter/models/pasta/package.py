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
import daiquiri

from gmn_adapter.config import Config
from gmn_adapter.exceptions import GMNAdapterDataPackageNotFound, GMNAdapterDataPackageResourcesNotFound
import gmn_adapter.iam.client as client
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
    rids = resource_registry.get_resource_ids(scope, identifier, revision)
    if len(rids) == 0:
        msg = f"No data package resources found: {scope}.{identifier}.{revision}"
        raise GMNAdapterDataPackageResourcesNotFound(msg)

    resource_ids = {}
    data_entities = []
    # Categorize resource ids into resource types
    for rid in rids:
        resource_id = rid[0]
        resource_type = rid[1]
        if resource_type == Config.PACKAGE:
            resource_ids[Config.PACKAGE] = resource_id
            resource_ids[Config.ORE] = resource_id + "?ore"
        elif resource_type == Config.METADATA:
            resource_ids[Config.METADATA] = resource_id
        elif resource_type == Config.REPORT:
            resource_ids[Config.REPORT] = resource_id
        elif resource_type == Config.DATA:
            data_entities.append(resource_id)
    resource_ids[Config.DATA] = data_entities

    return resource_ids


class Package:
    """
    Model a PASTA data package

    Args:
        pid (str): PASTA PID in the format scope.identifier.revision
    """

    def __init__(self, pid):
        """
        Construct relevant attributes for a PASTA data package, including
        1) package identifier
        2) package identifier parts (scope, identifier, revision)
        3) package DOI (must be present to be GMN candidate)
        4) package date deactivated (must be None to be GMN candidate)
        5) resource IDs for the package (all resources must be Public Access readable to be GMN candidate)

        Args:
            pid:

        Throws: ValueError, GMNAdapterDataPackageNotFound
        """
        try:
            self.scope, self.identifier, self.revision = pid.split(".")
            if len(self.scope) == 0: raise ValueError(f"Scope cannot be empty: {pid}")
            if len(self.identifier) == 0 or not self.identifier.isdigit():
                raise ValueError(f"Identifier must be a positive integer: {pid}")
            if len(self.revision) == 0 or not self.revision.isdigit():
                raise ValueError(f"Revision must be a positive integer: {pid}")
        except ValueError as e:
            msg = f"Invalid PID format: {pid}"
            logger.error(msg)
            raise ValueError(msg) from e

        resource_registry = ResourceRegistry()
        self.pid = pid
        try:
            self.resource_ids = _get_package_resource_ids(resource_registry, self.scope, self.identifier, self.revision)
        except GMNAdapterDataPackageResourcesNotFound as e:
            msg = f"Data package not found: {pid}"
            raise GMNAdapterDataPackageNotFound(msg) from e
        self.doi = resource_registry.get_package_doi(self.scope, self.identifier, self.revision)[0][0]
        self.date_deactivated = resource_registry.get_date_deactivated(self.scope, self.identifier, self.revision)[0][0]
        self.is_gmn_candidate = self._is_gmn_candidate()

    def _is_gmn_candidate(self) -> bool:
        """
        Fail fast if any condition is not met to be a GMN candidate.

        Returns:
            bool: True if the package is a GMN candidate, False otherwise.
        """
        if self.doi is None:
            return False

        if self.date_deactivated is not None:
            return False

        data_entities = self.resource_ids[Config.DATA]
        if len(data_entities) == 0:
            return False

        public_token = client.get_public_token()

        if not client.is_authorized(public_token, self.resource_ids[Config.PACKAGE], "read"):
            return False

        if not client.is_authorized(public_token, self.resource_ids[Config.METADATA], "read"):
            return False

        if not client.is_authorized(public_token, self.resource_ids[Config.REPORT], "read"):
            return False

        for entity in data_entities:
            if not client.is_authorized(public_token, entity, "read"):
                return False

        return True




