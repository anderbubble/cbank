"""The clusterbank model.

This package contains the local data model, including reflected projects
and resources from upstream, along with requests, allocations, charges, etc.

Classes:
Project -- a project that can use a resource
Resource -- a resource that can be allocated
Request -- request for an allocation
Allocation -- allocation of a resource to a project
Hold -- hold of funds from an account
Charge -- charge against an account
Refund -- refund of a charge

Objects:
metadata -- metadata collection
Session -- sessionmaker (and default session)

metadata will be automatically bound to an engine specified in a config
file if present.

Configuration:
/etc/clusterbank.conf -- [main] database
"""

import warnings
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, relation, synonym
from sqlalchemy.orm.session import SessionExtension

from clusterbank.model.metadata import metadata, \
    users_table, projects_table, resources_table, \
    requests_table, requests_allocations_table, allocations_table, credit_limits_table, \
    holds_table, charges_table, refunds_table
from clusterbank.model.entities import User, Project, Resource, Request, Allocation, CreditLimit, Hold, Charge, Refund
    

__all__ = [
    "Session",
    "User", "Project", "Resource",
    "Request", "Allocation", "CreditLimit", "Hold", "Charge", "Refund",
]

config = SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    uri = config.get("main", "database")
except (NoSectionError, NoOptionError):
    warnings.warn("no database specified", UserWarning)
else:
    try:
        metadata.bind = create_engine(uri)
    except Exception, e:
        warnings.warn("invalid database: %s (%s)" % (uri, e), UserWarning)


class MultiplexingSessionExtension (SessionExtension):
    
    def __init__ (self, extensions=None):
        SessionExtension.__init__(self)
        if extensions:
            self.extensions = extensions[:]
        else:
            self.extensions = []
    
    def before_commit (self, session):
        for extension in self.extensions:
            extension.before_commit(session)

    def after_commit (self, session):
        for extension in self.extensions:
            extension.after_commit(session)

    def after_rollback (self, session):
        for extension in self.extensions:
            extension.after_rollback(session)

    def before_flush (self, session, flush_context, objects):
        for extension in self.extensions:
            extension.before_flush(session, flush_context, objects)

    def after_flush (self, session, flush_context):
        for extension in self.extensions:
            extension.after_flush(session, flush_context)
        
    def after_flush_postexec (self, session, flush_context):
        for extension in self.extensions:
            extension.after_flush_postexec(session, flush_context)


Session = scoped_session(sessionmaker(
    transactional=True, autoflush=True,
    extension=MultiplexingSessionExtension([
        Request.SessionExtension(), Allocation.SessionExtension(),
        Hold.SessionExtension(), Charge.SessionExtension(),
        Refund.SessionExtension(), CreditLimit.SessionExtension(),
    ])))

Session.mapper(User, users_table, properties=dict(
    id = users_table.c.id,
))

Session.mapper(Project, projects_table, properties=dict(
    id = projects_table.c.id,
))

Session.mapper(Resource, resources_table, properties=dict(
    id = resources_table.c.id,
))

Session.mapper(Request, requests_table, properties=dict(
    id = requests_table.c.id,
    project = relation(Project, backref="requests"),
    resource = relation(Resource, backref="requests"),
    datetime = requests_table.c.datetime,
    amount = requests_table.c.amount,
    comment = requests_table.c.comment,
    start = requests_table.c.start,
))

Session.mapper(Allocation, allocations_table, properties=dict(
    id = allocations_table.c.id,
    project = relation(Project, backref="allocations"),
    resource = relation(Resource, backref="allocations"),
    datetime = allocations_table.c.datetime,
    amount = allocations_table.c.amount,
    start = allocations_table.c.start,
    expiration = allocations_table.c.expiration,
    comment = allocations_table.c.comment,
    requests = relation(Request, secondary=requests_allocations_table, backref="allocations"),
))

Session.mapper(CreditLimit, credit_limits_table, properties=dict(
    id = credit_limits_table.c.id,
    project = relation(Project, backref="credit_limits"),
    resource = relation(Resource, backref="credit_limits"),
    start = credit_limits_table.c.start,
    datetime = credit_limits_table.c.datetime,
    amount = credit_limits_table.c.amount,
    comment = credit_limits_table.c.comment,
))

Session.mapper(Hold, holds_table, properties=dict(
    id = holds_table.c.id,
    allocation = relation(Allocation, backref="holds"),
    datetime = holds_table.c.datetime,
    amount = holds_table.c.amount,
    comment = holds_table.c.comment,
    active = holds_table.c.active,
))

Session.mapper(Charge, charges_table, properties=dict(
    id = charges_table.c.id,
    allocation = relation(Allocation, backref="charges"),
    datetime = charges_table.c.datetime,
    amount = charges_table.c.amount,
    comment = charges_table.c.comment,
))

Session.mapper(Refund, refunds_table, properties=dict(
    id = refunds_table.c.id,
    charge = relation(Charge, backref="refunds"),
    datetime = refunds_table.c.datetime,
    amount = refunds_table.c.amount,
    comment = refunds_table.c.comment,
))
