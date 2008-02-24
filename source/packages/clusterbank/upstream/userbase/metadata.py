"""userbase metadata

Objects:
metadata -- master metadata object
projects_table -- userbase projects
resource_types_table -- userbase resources
"""

from sqlalchemy import MetaData, Table, Column, ForeignKey, types

try:
    types.Text
except NameError:
    types.Text = types.TEXT

__all__ = [
    "metadata",
    "projects_table",
    "resource_types_table",
]

metadata = MetaData()

projects_table = Table("projects", metadata,
    Column("project_id", types.Integer, primary_key=True),
    Column("project_name", types.Text, nullable=False, unique=True),
)

resource_types_table = Table("resource_types", metadata,
    Column("resource_id", types.Integer, nullable=False, primary_key=True),
    Column("resource_name", types.Text, nullable=False, unique=True),
)
