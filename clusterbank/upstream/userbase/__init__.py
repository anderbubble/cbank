"""Userbase upstream module.

The userbase module uses SQLAlchemy to build an interface over
the MCS userbase database.
"""

from django.conf import settings

# Connect the schema to the database.
import schema
schema.metadata.connect(settings.UPSTREAM_DATABASE_URI)

# Bring the plugin interface to the top level.
from model import User, Project, Resource, DoesNotExist
    
__all__ = ["schema", "model",
    "User", "Project", "Resource", "DoesNotExist"]

