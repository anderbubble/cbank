"""clusterbank data model metadata

The metadata provides sqlalchemy with information as to how the database
is layed out, so that it can map local classes to a relational database.

Objects:
metadata -- master metadata object
projects_table -- projects
resources_table -- resources
requests_table -- requests
allocations_table -- allocations
credit_limits_table -- credit limits
holds_table -- holds
charges_table -- charges
refunds_table -- refunds
"""

from datetime import datetime

from sqlalchemy import MetaData, Table, Column, ForeignKey, UniqueConstraint, types

__all__ = [
    "metadata",
    "projects_table", "resources_table",
    "requests_table", "allocations_table", "credit_limits_table",
    "holds_table", "charges_table", "refunds_table",
]

metadata = MetaData()

projects_table = Table("projects", metadata,
    Column("id", types.Integer, primary_key=True),
)

resources_table = Table("resources", metadata,
    Column("id", types.Integer, primary_key=True),
)

requests_table = Table("requests", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("project_id", None, ForeignKey("projects.id"), nullable=False),
    Column("resource_id", None, ForeignKey("resources.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("start", types.DateTime, nullable=True),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.String, nullable=True),
    Column("allocation_id", None, ForeignKey("allocations.id"), nullable=True),
)

allocations_table = Table("allocations", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("project_id", None, ForeignKey("projects.id"), nullable=False),
    Column("resource_id", None, ForeignKey("resources.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("start", types.DateTime, nullable=False),
    Column("expiration", types.DateTime, nullable=False),
    Column("comment", types.String),
)

credit_limits_table = Table("credit_limits", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("project_id", None, ForeignKey("projects.id"), nullable=False),
    Column("resource_id", None, ForeignKey("resources.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("start", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.String),
    UniqueConstraint("project_id", "resource_id", "start"),
)

holds_table = Table("holds", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("allocation_id", None, ForeignKey("allocations.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.String),
    Column("active", types.Boolean, nullable=False, default=True),
)

charges_table = Table("charges", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("allocation_id", None, ForeignKey("allocations.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.String),
)

refunds_table = Table("refunds", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("charge_id", None, ForeignKey("charges.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.String),
)
