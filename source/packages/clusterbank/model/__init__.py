import warnings
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, relation, synonym

from clusterbank.model.metadata import \
    metadata, \
    users_table, projects_table, resources_table, \
    credit_limits_table, requests_table, allocations_table, \
    liens_table, charges_table, refunds_table
from clusterbank.model.entities import \
    User, Project, Resource
from clusterbank.model.accounting import \
    CreditLimit, Request, Allocation, Lien, Charge, Refund

__all__ = [
    "Session",
    "User", "Project", "Resource",
    "CreditLimit", "Request", "Allocation", "Lien", "Charge", "Refund",
]

config = SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    uri = config.get("main", "database")
except (NoSectionError, NoOptionError):
    warnings.warn("no database specified", ImportWarning)
else:
    try:
        metadata.bind = create_engine(uri)
    except Exception, e:
        warnings.warn("invalid database: %s (%s)" % (uri, e), ImportWarning)

Session = scoped_session(sessionmaker(transactional=True, autoflush=True))

Session.mapper(User, users_table, properties=dict(
    id = users_table.c.id,
    can_request = users_table.c.can_request,
    can_allocate = users_table.c.can_allocate,
    can_lien = users_table.c.can_lien,
    can_charge = users_table.c.can_charge,
    can_refund = users_table.c.can_refund,
    requests = relation(Request, backref="_poster"),
    allocations = relation(Allocation, backref="_poster"),
    credit_limits = relation(CreditLimit, backref="_poster"),
    liens = relation(Lien, backref="_poster"),
    charges = relation(Charge, backref="_poster"),
    refunds = relation(Refund, backref="_poster"),
))

Session.mapper(Project, projects_table, properties=dict(
    id = projects_table.c.id,
    credit_limits = relation(CreditLimit, backref="project"),
))

Session.mapper(Resource, resources_table, properties=dict(
    id = resources_table.c.id,
    credit_limits = relation(CreditLimit, backref="resource"),
))

Session.mapper(CreditLimit, credit_limits_table, properties=dict(
    id = credit_limits_table.c.id,
    project = relation(Project, backref="credit_limits"),
    resource = relation(Resource, backref="credit_limits"),
    _poster = relation(User, backref="credit_limits"),
    poster = synonym("_poster"),
    start = credit_limits_table.c.start,
    _time = credit_limits_table.c.time,
    time = synonym("_time"),
    comment = credit_limits_table.c.comment,
))

Session.mapper(Request, requests_table, properties=dict(
    id = requests_table.c.id,
    resource = relation(Resource, backref="requests"),
    _project = relation(Project, backref="requests"),
    project = synonym("_project"),
    _poster = relation(User, backref="requests"),
    poster = synonym("_poster"),
    datetime = requests_table.c.datetime,
    _time = requests_table.c.time,
    time = synonym("_time"),
    comment = requests_table.c.comment,
    start = requests_table.c.start,
    allocations = relation(Allocation, backref="request"),
))

Session.mapper(Allocation, allocations_table, properties=dict(
    id = allocations_table.c.id,
    request = relation(Request, backref="allocations"),
    _poster = relation(User, backref="allocations"),
    poster = synonym("_poster"),
    approver = allocations_table.c.approver,
    datetime = allocations_table.c.datetime,
    _time = allocations_table.c.time,
    time = synonym("_time"),
    start = allocations_table.c.start,
    expiration = allocations_table.c.expiration,
    comment = allocations_table.c.comment,
    liens = relation(Lien, backref="allocation"),
))

Session.mapper(Lien, liens_table, properties=dict(
    id = liens_table.c.id,
    allocation = relation(Allocation, backref="liens"),
    _poster = relation(User, backref="liens"),
    poster = synonym("_poster"),
    datetime = liens_table.c.datetime,
    _time = liens_table.c.time,
    time = synonym("_time"),
    comment = liens_table.c.comment,
    charges = relation(Charge, backref="lien"),
))

Session.mapper(Charge, charges_table, properties=dict(
    id = charges_table.c.id,
    lien = relation(Lien, backref="charges"),
    _poster = relation(User, backref="charges"),
    poster = synonym("_poster"),
    datetime = charges_table.c.datetime,
    _time = charges_table.c.time,
    time = synonym("_time"),
    comment = charges_table.c.comment,
    refunds = relation(Refund, backref="_charge"),
))

Session.mapper(Refund, refunds_table, properties=dict(
    id = refunds_table.c.id,
    _charge = relation(Charge, backref="refunds"),
    charge = synonym("_charge"),
    _poster = relation(User, backref="refunds"),
    poster = synonym("_poster"),
    datetime = refunds_table.c.datetime,
    _time = refunds_table.c.time,
    time = synonym("_time"),
    comment = refunds_table.c.comment,
))
