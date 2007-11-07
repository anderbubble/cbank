import warnings
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, relation, synonym

from clusterbank.model.metadata import \
    metadata, \
    projects_table, resources_table, \
    credit_limits_table, requests_table, allocations_table, \
    liens_table, charges_table, refunds_table
from clusterbank.model.entities import \
    Project, Resource
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
    warnings.warn("no database specified", UserWarning)
else:
    try:
        metadata.bind = create_engine(uri)
    except Exception, e:
        warnings.warn("invalid database: %s (%s)" % (uri, e), UserWarning)

Session = scoped_session(sessionmaker(transactional=True, autoflush=True))

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
    start = credit_limits_table.c.start,
    _amount = credit_limits_table.c.amount,
    amount = synonym("_amount"),
    comment = credit_limits_table.c.comment,
))

Session.mapper(Request, requests_table, properties=dict(
    id = requests_table.c.id,
    resource = relation(Resource, backref="requests"),
    project = relation(Project, backref="requests"),
    datetime = requests_table.c.datetime,
    _amount = requests_table.c.amount,
    amount = synonym("_amount"),
    comment = requests_table.c.comment,
    start = requests_table.c.start,
    allocations = relation(Allocation, backref="request"),
))

Session.mapper(Allocation, allocations_table, properties=dict(
    id = allocations_table.c.id,
    request = relation(Request, backref="allocations"),
    approver = allocations_table.c.approver,
    datetime = allocations_table.c.datetime,
    _amount = allocations_table.c.amount,
    amount = synonym("_amount"),
    start = allocations_table.c.start,
    expiration = allocations_table.c.expiration,
    comment = allocations_table.c.comment,
    liens = relation(Lien, backref="allocation"),
))

Session.mapper(Lien, liens_table, properties=dict(
    id = liens_table.c.id,
    allocation = relation(Allocation, backref="liens"),
    datetime = liens_table.c.datetime,
    _amount = liens_table.c.amount,
    amount = synonym("_amount"),
    comment = liens_table.c.comment,
    charges = relation(Charge, backref="lien"),
))

Session.mapper(Charge, charges_table, properties=dict(
    id = charges_table.c.id,
    lien = relation(Lien, backref="charges"),
    datetime = charges_table.c.datetime,
    _amount = charges_table.c.amount,
    amount = synonym("_amount"),
    comment = charges_table.c.comment,
    refunds = relation(Refund, backref="_charge"),
))

Session.mapper(Refund, refunds_table, properties=dict(
    id = refunds_table.c.id,
    _charge = relation(Charge, backref="refunds"),
    charge = synonym("_charge"),
    datetime = refunds_table.c.datetime,
    _amount = refunds_table.c.amount,
    amount = synonym("_amount"),
    comment = refunds_table.c.comment,
))
