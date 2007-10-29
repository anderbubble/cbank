from sqlalchemy import MetaData, Table, Column, ForeignKey, types

__all__ = [
    "metadata",
    "user_table", "projects_table",
    "project_members_table", "resource_types_table",
]

metadata = MetaData()

user_table = Table("user", metadata,
    Column("userbase_id", types.Integer, nullable=False, primary_key=True),
    Column("username", types.String, nullable=False, unique=True),
)

projects_table = Table("projects", metadata,
    Column("project_id", types.Integer, primary_key=True),
    Column("project_name", types.String, nullable=False, unique=True),
)

project_members_table = Table("project_members", metadata,
    Column("userbase_id", None, ForeignKey("user.userbase_id"), primary_key=True),
    Column("project_id", None, ForeignKey("projects.project_id"), primary_key=True),
)

resource_types_table = Table("resource_types", metadata,
    Column("resource_id", types.Integer, nullable=False, primary_key=True),
    Column("resource_name", types.String, nullable=False, unique=True),
)
