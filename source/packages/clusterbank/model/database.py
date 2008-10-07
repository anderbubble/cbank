"""clusterbank data model metadata

The metadata provides sqlalchemy with information as to how the database
is layed out, so that it can map local classes to a relational database.

Objects:
metadata -- master metadata object
projects -- projects
resources -- resources
requests -- requests
allocations -- allocations
credit_limits -- credit limits
holds -- holds
jobs -- jobs run on a resource
charges -- charges
refunds -- refunds
"""

from datetime import datetime

from sqlalchemy import MetaData, Table, Column, ForeignKey, \
    ForeignKeyConstraint, UniqueConstraint, types

__all__ = [
    "metadata",
    "users", "projects", "resources",
    "requests", "allocations", "credit_limits",
    "holds", "jobs", "charges", "jobs_charges", "refunds",
]

metadata = MetaData()

users = Table("users", metadata,
    Column("id", types.Integer, primary_key=True),
    mysql_engine="InnoDB")

projects = Table("projects", metadata,
    Column("id", types.Integer, primary_key=True),
    mysql_engine="InnoDB")

resources = Table("resources", metadata,
    Column("id", types.Integer, primary_key=True),
    mysql_engine="InnoDB")

requests = Table("requests", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("project_id", None, ForeignKey("projects.id"), nullable=False),
    Column("resource_id", None, ForeignKey("resources.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("start", types.DateTime, nullable=True),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.Text, nullable=True),
    mysql_engine="InnoDB")

allocations = Table("allocations", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("project_id", None, ForeignKey("projects.id"), nullable=False),
    Column("resource_id", None, ForeignKey("resources.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("start", types.DateTime, nullable=False),
    Column("expiration", types.DateTime, nullable=False),
    Column("comment", types.Text),
    mysql_engine="InnoDB")

requests_allocations = Table("requests_allocations", metadata,
    Column("request_id", None, ForeignKey("requests.id"), primary_key=True),
    Column("allocation_id", None, ForeignKey("allocations.id"),
        primary_key=True),
    mysql_engine="InnoDB")

credit_limits = Table("credit_limits", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("project_id", None, ForeignKey("projects.id"), nullable=False),
    Column("resource_id", None, ForeignKey("resources.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("start", types.DateTime, nullable=False),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.Text),
    UniqueConstraint("project_id", "resource_id", "start"),
    mysql_engine="InnoDB")

holds = Table("holds", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("allocation_id", None, ForeignKey("allocations.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("user_id", None, ForeignKey("users.id"), nullable=True),
    Column("comment", types.Text),
    Column("active", types.Boolean, nullable=False, default=True),
    mysql_engine="InnoDB")

jobs = Table("jobs", metadata,
    Column("resource_id", None, ForeignKey("resources.id"), primary_key=True),
    Column("id", types.Integer, primary_key=True, autoincrement=False),
    Column("start", types.DateTime),
    Column("end", types.DateTime),
    mysql_engine="InnoDB")

charges = Table("charges", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("allocation_id", None, ForeignKey("allocations.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("user_id", None, ForeignKey("users.id"), nullable=True),
    Column("comment", types.Text),
    mysql_engine="InnoDB")

jobs_charges = Table("jobs_charges", metadata,
    Column("job_resource_id", types.Integer),
    Column("job_id", types.Integer),
    Column("charge_id", None, ForeignKey("charges.id")),
    ForeignKeyConstraint(["job_resource_id", "job_id"],
        ["jobs.resource_id", "jobs.id"]),
    mysql_engine="InnoDB")

refunds = Table("refunds", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("charge_id", None, ForeignKey("charges.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.Text),
    mysql_engine="InnoDB")

