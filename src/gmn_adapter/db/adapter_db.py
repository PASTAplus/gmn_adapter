#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Database model to support queueing objects from PASTA for use by the GMN Adapter.

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

import os

import daiquiri
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import desc
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from gmn_adapter.config import Config


logger = daiquiri.getLogger(__name__)

Base = declarative_base()


class Queue(Base):
    """SQLAlchemy ORM model for the adapter queue."""

    __tablename__ = "queue"

    package = Column(String, primary_key=True)
    scope = Column(String, nullable=False)
    identifier = Column(Integer, nullable=False)
    revision = Column(Integer, nullable=False)
    method = Column(String, primary_key=True)
    datetime = Column(DateTime, nullable=False)
    owner = Column(String, nullable=False)
    doi = Column(String, nullable=False)
    dequeued = Column(Boolean, nullable=False, default=False)


class QueueManager(object):
    """Queue management for the adapter queue."""

    def __init__(self, queue=Config.QUEUE):
        """Initialize a queue manager backed by an SQLite database.

        Args:
            queue: Path to the SQLite database file for the queue.
        """
        self.queue = queue
        db = "sqlite+pysqlite:///" + self.queue
        engine = create_engine(db)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def delete_queue(self):
        """Remove the SQLite database file from the filesystem."""
        os.remove(self.queue)

    def dequeue(self, package=None, method=None):
        """Mark a queued PASTA event as dequeued.

        Args:
            package: The PASTA data package identifier (e.g., "scope.id.rev").
            method: The PASTA event method type.

        Returns:
            None
        """
        try:
            event = (
                self.session.query(Queue)
                .filter(Queue.package == package, Queue.method == method)
                .one()
            )
            event.dequeued = True
            self.session.commit()
        except NoResultFound as e:
            p = package
            logger.error(f"{e} - {p}")

    def enqueue(self, event=None):
        """Insert a PASTA event into the adapter queue.

        Args:
            event: Instance of the utility `Event` class.

        Returns:
            None
        """
        scope, identifier, revision = event.package.split(".")

        event = Queue(
            package=event.package,
            scope=scope,
            identifier=identifier,
            revision=revision,
            method=event.method,
            datetime=event.datetime,
            owner=event.owner,
            doi=event.doi,
        )
        try:
            self.session.add(event)
            self.session.commit()
        except IntegrityError as e:
            logger.error(e)
            self.session.rollback()

    def get_count(self):
        """Return the number of records in the adapter queue.

        Returns:
            int: Total count of queued event records.
        """
        return self.session.query(func.count(Queue.package)).scalar()

    def get_event(self, package=None, method=None):
        """Return the queue event record for a given package and method.

        Args:
            package: The PASTA data package identifier (e.g., "scope.id.rev").
            method: The PASTA event method type.

        Returns:
            Queue | None: The matching queue event record, or `None` if not found.
        """
        try:
            return (
                self.session.query(Queue)
                .filter(Queue.package == package, Queue.method == method)
                .one()
            )
        except NoResultFound as e:
            p = package
            logger.error(f"{e} - {p}")

    def get_head(self):
        """Return the first not-yet-dequeued event record.

        Returns:
            Queue | None: Oldest non-dequeued event record, or `None` if none exist.
        """
        return (
            self.session.query(Queue)
            .filter(Queue.dequeued == False)
            .order_by(Queue.datetime)
            .first()
        )

    def get_last_datetime(self):
        """Return the datetime of the most recent queue entry.

        Returns:
            datetime.datetime | None: Datetime of the last queue entry, or `None` if empty.
        """
        datetime = None
        event = self.session.query(Queue).order_by(desc(Queue.datetime)).first()
        if event is not None:
            datetime = event.datetime
        return datetime

    def get_predecessor(self, package=None):
        """Return the most recent predecessor for a given package.

        A predecessor is an event with the same scope and identifier, but a lower
        revision number.

        Args:
            package: The package identifier (e.g., "scope.id.rev").

        Returns:
            Queue | None: The predecessor event record, or `None` if none found.
        """
        scope, identifier, revision = package.split(".")
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

    def is_dequeued(self, package=None, method=None):
        """Return whether the specified package/method has been dequeued.

        Args:
            package: The PASTA data package identifier (e.g., "scope.id.rev").
            method: The PASTA event method type.

        Returns:
            bool | None: Dequeued status, or `None` if the record is not found.
        """
        dequeued = None
        try:
            event = (
                self.session.query(Queue)
                .filter(Queue.package == package, Queue.method == method)
                .one()
            )
            dequeued = event.dequeued
        except NoResultFound as e:
            p = package
            logger.error(f"{e} - {p}")
        return dequeued


def main():
    """Program entry point.

    Returns:
        int: Process exit code.
    """
    return 0


if __name__ == "__main__":
    main()
