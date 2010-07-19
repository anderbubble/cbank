import warnings
import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.orm import mapper, relation
from sqlalchemy.exceptions import ArgumentError

from cbank import config
from cbank.model.entities import (
    User, Project, Resource,
    Allocation, Hold, Job, Charge, Refund)
from cbank.model.database import (
    metadata, allocations, holds, jobs, charges, refunds)
from cbank.model.queries import (
    Session, get_projects, get_users, import_job,
    user_summary, project_summary)


__all__ = [
    "User", "Project", "Resource",
    "Allocation", "Hold", "Job", "Charge", "Refund",
    "Session", "get_projects", "get_users", "import_job",
    "user_summary", "project_summary"]


def configured_engine ():
    """Build a configured SQLAlchemy engine."""
    try:
        uri = config.get("main", "database")
    except ConfigParser.Error:
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
        module_name = "cbank.upstreams.default"
    try:
        module = __import__(module_name, locals(), globals(), [
            "user_in", "user_out",
            "project_in", "project_out",
            "resource_in", "resource_out",
            "project_member", "project_manager"])
    except ImportError:
        warnings.warn(
            "invalid upstream module: %s" % (module_name), UserWarning)
        module = None
    return module


mapper(Allocation, allocations, properties={
    'id':allocations.c.id,
    'project_id':allocations.c.project_id,
    'resource_id':allocations.c.resource_id,
    'datetime':allocations.c.datetime,
    'amount':allocations.c.amount,
    'start':allocations.c.start,
    'end':allocations.c.end,
    'comment':allocations.c.comment})


mapper(Hold, holds, properties={
    'id':holds.c.id,
    'allocation':relation(Allocation, backref="holds"),
    'datetime':holds.c.datetime,
    'amount':holds.c.amount,
    'comment':holds.c.comment,
    'active':holds.c.active,
    'job':relation(Job, backref="holds")})


mapper(Job, jobs, properties={
    'id':jobs.c.id,
    'user_id':jobs.c.user_id,
    'group':jobs.c.group,
    'account_id':jobs.c.account_id,
    'name':jobs.c.name,
    'queue':jobs.c.queue,
    'reservation_name':jobs.c.reservation_name,
    'reservation_id':jobs.c.reservation_id,
    'ctime':jobs.c.ctime,
    'qtime':jobs.c.qtime,
    'etime':jobs.c.etime,
    'start':jobs.c.start,
    'exec_host':jobs.c.exec_host,
    'resource_list':jobs.c.resource_list,
    'session':jobs.c.session,
    'alternate_id':jobs.c.alternate_id,
    'end':jobs.c.end,
    'exit_status':jobs.c.exit_status,
    'resources_used':jobs.c.resources_used,
    'accounting_id':jobs.c.accounting_id})


mapper(Charge, charges, properties={
    'id':charges.c.id,
    'allocation':relation(Allocation, backref="charges"),
    'datetime':charges.c.datetime,
    'amount':charges.c.amount,
    'comment':charges.c.comment,
    'job':relation(Job, backref="charges"),
    'refunds':relation(Refund, backref="charge", cascade="all")})


mapper(Refund, refunds, properties={
    'id':refunds.c.id,
    'datetime':refunds.c.datetime,
    'amount':refunds.c.amount,
    'comment':refunds.c.comment})


def use_upstream (upstream):
    """untested"""
    User._in = staticmethod(upstream.user_in)
    User._out = staticmethod(upstream.user_out)
    User._member = staticmethod(upstream.project_member)
    User._manager = staticmethod(upstream.project_manager)
    Project._in = staticmethod(upstream.project_in)
    Project._out = staticmethod(upstream.project_out)
    Resource._in = staticmethod(upstream.resource_in)
    Resource._out = staticmethod(upstream.resource_out)


def clear_upstream ():
    """untested"""
    User._in = None
    User._out = None
    User._member = None
    User._manager = None
    Project._in = None
    Project._out = None
    Resource._in = None
    Resource._out = None


metadata.bind = configured_engine()
use_upstream(configured_upstream())
