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
import sqlalchemy.exceptions
from sqlalchemy.orm import scoped_session, sessionmaker, mapper, relation, synonym
from sqlalchemy.orm.session import SessionExtension

from clusterbank.model.entities import upstream, User, Project, Resource, Request, Allocation, CreditLimit, Hold, Charge, Refund
from clusterbank.model.database import metadata, \
    users_table, projects_table, resources_table, requests_table, \
    requests_allocations_table, allocations_table, credit_limits_table, \
    holds_table, charges_table, refunds_table
import clusterbank.exceptions as exceptions

__all__ = [
    "upstream", "Session",
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
try:
    upstream_module_name = config.get("upstream", "module")
except (NoSectionError, NoOptionError):
    upstream_module_name = "clusterbank.upstreams.default"
try:
    upstream.use = __import__(upstream_module_name, locals(), globals(), ["get_project_name", "get_project_id", "get_resource_name", "get_resource_id"])
except ImportError:
    warnings.warn("invalid upstream module: %s" % (upstream_module_name), UserWarning)

class SessionConstraints (SessionExtension):
    
    def forbid_negative_amounts (self, session):
        for entity in (session.new | session.dirty):
            if isinstance(entity, Request):
                if entity.amount < 0:
                    raise ValueError("invalid amount for request: %r" % entity.amount)
            elif isinstance(entity, Allocation):
                if entity.amount < 0:
                    raise ValueError("invalid amount for allocation: %r" % entity.amount)
            elif isinstance(entity, CreditLimit):
                if entity.amount < 0:
                    raise ValueError("invalid amount for credit limit: %r" % entity.amount)
            elif isinstance(entity, Hold):
                if entity.amount < 0:
                    raise ValueError("invalid amount for hold: %r" % entity.amount)
            elif isinstance(entity, Charge):
                if entity.amount < 0:
                    raise ValueError("invalid amount for charge: %r" % entity.amount)
            elif isinstance(entity, Refund):
                if entity.amount < 0:
                    raise ValueError("invalid amount for refund: %r" % entity.amount)
    
    def forbid_holds_greater_than_allocation (self, session):
        holds = [instance for instance in (session.new | session.dirty) if isinstance(instance, Hold)]
        for allocation in set([hold.allocation for hold in holds]):
            credit_limit = allocation.project.credit_limit(allocation.resource)
            if credit_limit:
                credit_limit = credit_limit.amount
            else:
                credit_limit = 0
            if allocation.amount_available < -credit_limit:
                raise exceptions.InsufficientFunds("not enough funds available")
    
    def forbid_refunds_greater_than_charge (self, session):
        refunds = [instance for instance in (session.new | session.dirty) if isinstance(instance, Refund)]
        charges = set([refund.charge for refund in refunds])
        for charge in charges:
            if charge.effective_amount < 0:
                raise exceptions.InsufficientFunds("not enough funds available")
    
    def before_commit (self, session):
        self.forbid_negative_amounts(session)
        self.forbid_holds_greater_than_allocation(session)
        self.forbid_refunds_greater_than_charge(session)

Session = scoped_session(sessionmaker(transactional=True, autoflush=True,
    extension=SessionConstraints()))

mapper(User, users_table, properties=dict(
    id = users_table.c.id,
))

mapper(Project, projects_table, properties=dict(
    id = projects_table.c.id,
))

mapper(Resource, resources_table, properties=dict(
    id = resources_table.c.id,
))

mapper(Request, requests_table, properties=dict(
    id = requests_table.c.id,
    project = relation(Project, backref="requests"),
    resource = relation(Resource, backref="requests"),
    datetime = requests_table.c.datetime,
    amount = requests_table.c.amount,
    comment = requests_table.c.comment,
    start = requests_table.c.start,
))

mapper(Allocation, allocations_table, properties=dict(
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

mapper(CreditLimit, credit_limits_table, properties=dict(
    id = credit_limits_table.c.id,
    project = relation(Project, backref="credit_limits"),
    resource = relation(Resource, backref="credit_limits"),
    start = credit_limits_table.c.start,
    datetime = credit_limits_table.c.datetime,
    amount = credit_limits_table.c.amount,
    comment = credit_limits_table.c.comment,
))

mapper(Hold, holds_table, properties=dict(
    id = holds_table.c.id,
    allocation = relation(Allocation, backref="holds"),
    datetime = holds_table.c.datetime,
    user = relation(User, backref="holds"),
    amount = holds_table.c.amount,
    comment = holds_table.c.comment,
    active = holds_table.c.active,
))

mapper(Charge, charges_table, properties=dict(
    id = charges_table.c.id,
    allocation = relation(Allocation, backref="charges"),
    datetime = charges_table.c.datetime,
    user = relation(User, backref="charges"),
    amount = charges_table.c.amount,
    comment = charges_table.c.comment,
))

mapper(Refund, refunds_table, properties=dict(
    id = refunds_table.c.id,
    charge = relation(Charge, backref="refunds"),
    datetime = refunds_table.c.datetime,
    amount = refunds_table.c.amount,
    comment = refunds_table.c.comment,
))

def _get_upstream_entity (cls, upstream_function, entity_name):
    upstream_id = upstream_function(entity_name)
    if upstream_id is None:
        raise exceptions.NotFound("%s '%s' not found" % (
            cls.__name__.lower(), entity_name))
    try:
        return Session.query(cls).filter_by(id=upstream_id).one()
    except sqlalchemy.exceptions.InvalidRequestError:
        entity = cls(id=upstream_id)
        Session.save(entity)
        return entity

def user_by_name (user_name):
    return _get_upstream_entity(User, upstream.get_user_id, user_name)

def project_by_name (project_name):
    return _get_upstream_entity(Project, upstream.get_project_id, project_name)

def resource_by_name (resource_name):
    return _get_upstream_entity(Resource, upstream.get_resource_id, resource_name)
