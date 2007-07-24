"""Userbase upstream module.

The userbase module uses SQLAlchemy to build an interface over
the MCS userbase database.
"""

from schema import metadata

# Bring the plugin interface to the top level.
from model import User, Project, Resource, DoesNotExist
    
__all__ = ["schema", "model",
    "User", "Project", "Resource", "DoesNotExist"]
