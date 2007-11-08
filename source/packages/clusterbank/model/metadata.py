from datetime import datetime

from sqlalchemy import MetaData, Table, Column, ForeignKey, UniqueConstraint, types

metadata = MetaData()

projects_table = Table("projects", metadata,
    Column("id", types.Integer, primary_key=True),
)

resources_table = Table("resources", metadata,
    Column("id", types.Integer, primary_key=True),
)

credit_limits_table = Table("credit_limits", metadata,
    Column("project_id", None, ForeignKey("projects.id")),
    Column("resource_id", None, ForeignKey("resources.id")),
    Column("id", types.Integer, primary_key=True),
    Column("start", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.String),
    UniqueConstraint("project_id", "resource_id", "start"),
)

requests_table = Table("requests", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("resource_id", None, ForeignKey("resources.id"), nullable=False),
    Column("project_id", None, ForeignKey("projects.id"), nullable=False),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("start", types.DateTime),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.String),
)

allocations_table = Table("allocations", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("request_id", None, ForeignKey("requests.id")),
    Column("approver", types.String),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("start", types.DateTime, nullable=False),
    Column("expiration", types.DateTime, nullable=False),
    Column("comment", types.String),
)

liens_table = Table("liens", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("allocation_id", None, ForeignKey("allocations.id")),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.String),
)

charges_table = Table("charges", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("lien_id", None, ForeignKey("liens.id")),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.String),
)

refunds_table = Table("refunds", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("charge_id", None, ForeignKey("charges.id")),
    Column("datetime", types.DateTime, nullable=False, default=datetime.now),
    Column("amount", types.Integer, nullable=False),
    Column("comment", types.String),
)
