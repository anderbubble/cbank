"""The clusterbank model.

This package contains the local data model, including reflected projects
and resources from upstream, along with requests, allocations, charges, etc.

Classes:
User -- a user that can charge
Project -- a project that can use a resource
Resource -- a resource that can be allocated
Request -- request for an allocation
Allocation -- allocation of a resource to a project
Hold -- hold of funds from an account
Job -- job run on a resource
Charge -- charge against an account
Refund -- refund of a charge

Objects:
metadata -- metadata collection
Session -- sessionmaker (and default session)
"""

import warnings
import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.sql import and_
from sqlalchemy.exceptions import InvalidRequestError, ArgumentError
from sqlalchemy.orm import scoped_session, sessionmaker, mapper, \
    relation
from sqlalchemy.orm.session import SessionExtension

from clusterbank import config
from clusterbank.model.entities import upstream, User, Project, \
    Resource, Request, Allocation, CreditLimit, Hold, Job, Charge, Refund
from clusterbank.model.database import metadata, \
    users, projects, resources, requests, \
    requests_allocations, allocations, credit_limits, \
    holds, jobs, charges, jobs_charges, refunds
from clusterbank.exceptions import InsufficientFunds, NotFound

__all__ = [
    "upstream", "Session",
    "User", "Project", "Resource",
    "Request", "Allocation", "CreditLimit", "Hold", "Job", "Charge", "Refund",
    "user_by_id", "user_by_name", "project_by_id", "project_by_name",
    "resource_by_id", "resource_by_name",
    "user_projects", "user_projects_owned", "project_members",
    "project_owners",
]

def get_configured_engine ():
    try:
        uri = config.get("main", "database")
    except ConfigParser.Error:
        warnings.warn("no database specified", UserWarning)
        engine = None
    else:
        try:
            engine = create_engine(uri)
        except (ImportError, ArgumentError), ex:
            warnings.warn("invalid database: %s (%s)" % (uri, ex), UserWarning)
            engine = None
    return engine

def get_configured_upstream ():
    try:
        upstream_module_name = config.get("upstream", "module")
    except ConfigParser.Error:
        upstream_module_name = "clusterbank.upstreams.default"
    try:
        module = __import__(upstream_module_name, locals(), globals(), [
            "get_project_name", "get_project_id", "get_resource_name",
            "get_resource_id"])
    except ImportError:
        warnings.warn("invalid upstream module: %s" % (upstream_module_name),
            UserWarning)
        module = None
    return module

class SessionConstraints (SessionExtension):
    
    def forbid_negative_amounts (self, session):
        for entity in (session.new | session.dirty):
            if isinstance(entity, Request):
                if entity.amount < 0:
                    raise ValueError(
                        "invalid amount for request: %r" % entity.amount)
            elif isinstance(entity, Allocation):
                if entity.amount < 0:
                    raise ValueError(
                        "invalid amount for allocation: %r" % entity.amount)
            elif isinstance(entity, CreditLimit):
                if entity.amount < 0:
                    raise ValueError(
                        "invalid amount for credit limit: %r" % entity.amount)
            elif isinstance(entity, Hold):
                if entity.amount < 0:
                    raise ValueError(
                        "invalid amount for hold: %r" % entity.amount)
            elif isinstance(entity, Charge):
                if entity.amount < 0:
                    raise ValueError(
                        "invalid amount for charge: %r" % entity.amount)
            elif isinstance(entity, Refund):
                if entity.amount < 0:
                    raise ValueError(
                        "invalid amount for refund: %r" % entity.amount)
    
    def forbid_holds_greater_than_allocation (self, session):
        holds = [instance for instance in (session.new | session.dirty)
            if isinstance(instance, Hold)]
        for allocation in set([hold.allocation for hold in holds]):
            credit_limit = allocation.project.credit_limit(allocation.resource)
            if credit_limit:
                credit_limit = credit_limit.amount
            else:
                credit_limit = 0
            if allocation.amount_available < -credit_limit:
                raise InsufficientFunds("not enough funds available")
    
    def forbid_refunds_greater_than_charge (self, session):
        refunds = [instance for instance in (session.new | session.dirty)
            if isinstance(instance, Refund)]
        charges = set([refund.charge for refund in refunds])
        for charge in charges:
            if charge.effective_amount < 0:
                raise InsufficientFunds("not enough funds available")
    
    def before_commit (self, session):
        self.forbid_negative_amounts(session)
        self.forbid_holds_greater_than_allocation(session)
        self.forbid_refunds_greater_than_charge(session)

Session = scoped_session(sessionmaker(extension=SessionConstraints()))

mapper(User, users, properties={
    'id':users.c.id})

mapper(Project, projects, properties={
    'id':projects.c.id})

mapper(Resource, resources, properties={
    'id':resources.c.id})

mapper(Request, requests, properties={
    'id':requests.c.id,
    'project':relation(Project, backref="requests"),
    'resource':relation(Resource, backref="requests"),
    'datetime':requests.c.datetime,
    'amount':requests.c.amount,
    'comment':requests.c.comment,
    'start':requests.c.start})

mapper(Allocation, allocations, properties={
    'id':allocations.c.id,
    'project':relation(Project, backref="allocations"),
    'resource':relation(Resource, backref="allocations"),
    'datetime':allocations.c.datetime,
    'amount':allocations.c.amount,
    'start':allocations.c.start,
    'expiration':allocations.c.expiration,
    'comment':allocations.c.comment,
    'requests':relation(Request, backref="allocations",
        secondary=requests_allocations)})

mapper(CreditLimit, credit_limits, properties={
    'id':credit_limits.c.id,
    'project':relation(Project, backref="credit_limits"),
    'resource':relation(Resource, backref="credit_limits"),
    'start':credit_limits.c.start,
    'datetime':credit_limits.c.datetime,
    'amount':credit_limits.c.amount,
    'comment':credit_limits.c.comment})

mapper(Hold, holds, properties={
    'id':holds.c.id,
    'allocation':relation(Allocation, backref="holds"),
    'datetime':holds.c.datetime,
    'user':relation(User, backref="holds"),
    'amount':holds.c.amount,
    'comment':holds.c.comment,
    'active':holds.c.active})

mapper(Job, jobs, properties={
    'id':jobs.c.id,
    'resource':relation(Resource, backref="jobs"),
    'charges':relation(Charge, backref="jobs", secondary=jobs_charges)})

mapper(Charge, charges, properties={
    'id':charges.c.id,
    'allocation':relation(Allocation, backref="charges"),
    'datetime':charges.c.datetime,
    'user':relation(User, backref="charges"),
    'amount':charges.c.amount,
    'comment':charges.c.comment})

mapper(Refund, refunds, properties={
    'id':refunds.c.id,
    'charge':relation(Charge, backref="refunds"),
    'datetime':refunds.c.datetime,
    'amount':refunds.c.amount,
    'comment':refunds.c.comment})

def _get_upstream_entity (cls, upstream_function, entity_name):
    upstream_id = upstream_function(entity_name)
    if upstream_id is None:
        raise NotFound("%s '%s' not found" % (
            cls.__name__.lower(), entity_name))
    s = Session()
    try:
        return s.query(cls).filter_by(id=upstream_id).one()
    except InvalidRequestError:
        entity = cls(id=upstream_id)
        s.add(entity)
        return entity

def user_by_id (user_id):
    s = Session()
    try:
        return s.query(User).filter_by(id=user_id).one()
    except InvalidRequestError:
        user = User(user_id)
        s.add(user)
        return user

def user_by_name (user_name):
    return _get_upstream_entity(User, upstream.get_user_id, user_name)

def user_projects (user):
    return [project_by_id(project_id) for project_id in user.projects]

def user_projects_owned (user):
    return [project_by_id(project_id) for project_id in user.projects_owned]

def project_by_id (project_id):
    s = Session()
    try:
        return s.query(Project).filter_by(id=project_id).one()
    except InvalidRequestError:
        project = Project(project_id)
        s.add(project)
        return project

def project_by_name (project_name):
    return _get_upstream_entity(
        Project, upstream.get_project_id, project_name)

def project_members (project):
    return [user_by_id(user_id) for user_id in project.members]

def project_owners (project):
    return [user_by_id(user_id) for user_id in project.owners]

def resource_by_id (resource_id):
    s = Session()
    try:
        return s.query(Resource).filter_by(id=resource_id).one()
    except InvalidRequestError:
        resource = Resource(resource_id)
        s.add(resource)
        return resource

def resource_by_name (resource_name):
    return _get_upstream_entity(
        Resource, upstream.get_resource_id, resource_name)

metadata.bind = get_configured_engine()
upstream.use = get_configured_upstream()

