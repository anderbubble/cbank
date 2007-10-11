"""Userbase upstream module.

The userbase module uses SQLAlchemy to build an interface over
the MCS userbase database.
"""

from ConfigParser import SafeConfigParser
import warnings

import sqlalchemy
from sqlalchemy import create_engine

import model
from model import User, Project, Resource
    
__all__ = [
    "User", "Project", "Resource",
]

config = SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    uri = config.get("userbase", "database")
except:
    warnings.warn("no userbase database configured", ImportWarning)
else:
    try:
        model.metadata.bind = create_engine(uri)
    except:
        warnings.warn("invalid upstream database: %s" % uri, ImportWarning)
