from sqlalchemy import MetaData, Table, Column, ForeignKey, types

__all__ = [
    "metadata",
    "projects_table",
    "project_members_table", "resource_types_table",
]

metadata = MetaData()

projects_table = Table("projects", metadata,
    Column("project_id", types.Integer, primary_key=True),
    Column("project_name", types.String, nullable=False, unique=True),
)

resource_types_table = Table("resource_types", metadata,
    Column("resource_id", types.Integer, nullable=False, primary_key=True),
    Column("resource_name", types.String, nullable=False, unique=True),
)
