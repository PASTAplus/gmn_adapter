#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Summary: Database model to support queueing objects from PASTA's resource_registry for
   use by the GMN Adapter.

Module:
    adapter_db

Attributes:
    logger: Module logger instance.
    Base: SQLAlchemy declarative base.

Author:
    Mark Servilla

Date:
    2025-12-14
"""
from datetime import datetime
from pathlib import Path

import daiquiri
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy import create_engine
from sqlalchemy import desc
from sqlalchemy import func
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query

from gmn_adapter.config import Config
from gmn_adapter.models.adapter.event import Event


logger = daiquiri.getLogger(__name__)
Base = declarative_base()


class Queue(Base):
    """SQLAlchemy ORM models for the adapter queue."""

    __tablename__ = "queue"

    package = Column(String, primary_key=True)
    scope = Column(String, nullable=False)
    identifier = Column(Integer, nullable=False)
    revision = Column(Integer, nullable=False)
    datetime = Column(DateTime, nullable=False)
    owner = Column(String, nullable=False)
    doi = Column(String, nullable=True)
    dequeued = Column(Boolean, nullable=False, default=False)


class DuplicateQueueEntryError(Exception):
    """Exception raised when a package already exists in the adapter queue."""
    def __init__(self, package, message="Package is already enqueued"):
        self.package = package
        self.message = f"{message}: {package}"
        super().__init__(self.message)


class QueueManager(object):
    """Queue management for the adapter queue."""

    def __init__(self, queue: str=Config.QUEUE):
        """Initialize a queue manager backed by an SQLite database.

        Args:
            queue: Path to the SQLite database file or ":memory:" for in-memory database.
        """
        self.queue = queue
        db = "sqlite+pysqlite:///" + self.queue
        self.engine = create_engine(db)
        Base.metadata.create_all(self.engine)
        session = sessionmaker(bind=self.engine)
        self.session = session()

    def delete_queue(self):
        """Remove the SQLite database file from the filesystem."""
        self.session.close()
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()
        if self.queue != ":memory:":
            Path(self.queue).unlink()

    def dequeue(self, package: str):
        """Mark an event as dequeued.

        This method is idempotent.

        Args:
            package: The PASTA data package identifier (e.g., "scope.id.rev").

        Returns:
            None

        Raises:
            sqlalchemy.orm.exc.NoResultFound: If the package is not found in the queue.
        """
        event = (
            self.session.query(Queue)
            .filter(Queue.package == package)
            .one()
        )
        event.dequeued = True
        self.session.commit()

    def enqueue(self, event: Event=None):
        """Insert a PASTA event into the adapter queue.

        Args:
            event: Instance of the models `Event` class.

        Returns:
            None

        Raises:
            DuplicateQueueEntryError
        """
        integrity_error = self.session.query(Queue).filter(Queue.package == event.package).count() > 0
        if integrity_error:
            raise DuplicateQueueEntryError(event.package)

        record = Queue(
            package=event.package,
            scope=event.scope,
            identifier=event.identifier,
            revision=event.revision,
            datetime=event.timestamp,
            owner=event.owner,
            doi=event.doi,
        )
        self.session.add(record)
        self.session.commit()

    def get_count(self) -> int:
        """Return the number of records in the adapter queue.

        Returns:
            int: Total count of queued event records.
        """
        return self.session.query(func.count(Queue.package)).scalar()

    def get_event(self, package: str=None) -> type[Queue]:
        """Return the queue event record for a given package identifier.

        Args:
            package: The PASTA data package identifier (e.g., "scope.id.rev").

        Returns:
            Query: The matching queue event record.

        Raises:
            sqlalchemy.orm.exc.NoResultFound: If the package is not found in the queue.
        """
        return (
            self.session.query(Queue)
            .filter(Queue.package == package)
            .one()
        )

    def get_head(self) -> type[Queue] | None:
        """Return the oldest non-dequeued event record.

        Returns:
            Query: Oldest non-dequeued event record, or None if no valid records exist.
        """
        return (
            self.session.query(Queue)
            .filter(Queue.dequeued == False)
            .order_by(Queue.datetime)
            .first()
        )

    def get_last_datetime(self) -> datetime:
        """Return the datetime of the most recent queue entry.

        Returns:
            datetime.datetime: Datetime of the last queue entry, or `None` if empty.
        """
        return (self.session.query(Queue)
                 .order_by(desc(Queue.datetime))
                 .first()
                 .datetime)

    def get_last_event(self) -> type[Queue] | None:
        """Return the most recent queue entry.

        Returns:
            Queue: The most recent queue entry, or `None` if empty.
        """
        return (self.session.query(Queue)
                 .order_by(desc(Queue.datetime))
                 .first())

    def get_predecessor(self, package: str) -> type[Queue] | None:
        """Return the most recent predecessor for a given package.

        A predecessor is an event with the same scope and identifier, but a lower
        revision number.

        Args:
            package: The package identifier (e.g., "scope.id.rev").

        Returns:
            Queue | None: The predecessor event record, or `None` if none found.
        """
        scope, _identifier, _revision = package.split(".")
        identifier = int(_identifier)
        revision = int(_revision)

        return (
            self.session.query(Queue)
            .filter(
                Queue.scope == scope,
                Queue.identifier == identifier,
                Queue.revision < revision,
            )
            .order_by(desc(Queue.revision))
            .first()
        )

    def is_dequeued(self, package: str) -> bool:
        """Return whether the specified package has been dequeued.

        Args:
            package: The PASTA data package identifier (e.g., "scope.id.rev").

        Returns:
            bool: Dequeued status.

        Raises:
            sqlalchemy.orm.exc.NoResultFound: If the package is not found in the queue.
        """
        event = (
            self.session.query(Queue)
            .filter(Queue.package == package)
            .one()
        )

        return bool(event.dequeued)

