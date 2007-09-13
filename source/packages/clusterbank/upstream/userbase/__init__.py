"""Userbase upstream module.

The userbase module uses SQLAlchemy to build an interface over
the MCS userbase database.
"""

# Bring the plugin interface to the top level.
from model import User, Project, Resource
    
__all__ = [
    "User", "Project", "Resource", "DoesNotExist"
]
