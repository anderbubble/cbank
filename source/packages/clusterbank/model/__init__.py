"""The clusterbank model.

This package contains the local data model, including reflected projects
and resources from upstream, along with allocations, charges, etc.

Classes:
User -- a user that can charge
Project -- a project that can use a resource
Resource -- a resource that can be allocated
Allocation -- allocation of a resource to a project
Hold -- hold of funds from an account
Job -- job run on a resource
Charge -- charge against an account
Refund -- refund of a charge

Objects:
metadata -- metadata collection
upstream -- the upstream module
"""

import warnings
import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.orm import mapper, relation
from sqlalchemy.exceptions import ArgumentError

from clusterbank import config
from clusterbank.model.entities import upstream, User, Project, \
    Resource, Allocation, Hold, Job, Charge, Refund
from clusterbank.model.database import metadata, \
    users, projects, resources, \
    allocations, holds, jobs, charges, jobs_charges, refunds


__all__ = ["upstream", "User", "Project", "Resource",
    "Allocation", "Hold", "Job", "Charge", "Refund"]


def configured_engine ():
    """Build a configured SQLAlchemy engine."""
    try:
        uri = config.get("main", "database")
    except ConfigParser.Error:
        warnings.warn("no database specified", UserWarning)
        engine = None
    else:
        try:
            engine = create_engine(uri)
        except (ImportError, ArgumentError), ex:
            warnings.warn(
                "invalid database: %s (%s)" % (uri, ex), UserWarning)
            engine = None
    return engine


def configured_upstream ():
    """Import the configured upstream module."""
    try:
        module_name = config.get("upstream", "module")
    except ConfigParser.Error:
        module_name = "clusterbank.upstreams.default"
    try:
        module = __import__(module_name, locals(), globals(), [
            "get_user_name", "get_user_id",
            "get_project_name", "get_project_id",
            "get_resource_name", "get_resource_id",
            "get_project_members", "get_project_owners",
            "get_member_projects", "get_owner_projects"])
    except ImportError:
        warnings.warn(
            "invalid upstream module: %s" % (module_name), UserWarning)
        module = None
    return module


mapper(User, users, properties={
    'id':users.c.id})


mapper(Project, projects, properties={
    'id':projects.c.id})


mapper(Resource, resources, properties={
    'id':resources.c.id})


mapper(Allocation, allocations, properties={
    'id':allocations.c.id,
    'project':relation(Project, backref="allocations"),
    'resource':relation(Resource, backref="allocations"),
    'datetime':allocations.c.datetime,
    'amount':allocations.c.amount,
    'start':allocations.c.start,
    'expiration':allocations.c.expiration,
    'comment':allocations.c.comment})


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

metadata.bind = configured_engine()
upstream.use = configured_upstream()
