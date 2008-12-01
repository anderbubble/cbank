"""clusterbank

A library for managing and accounting for the use of resources.
"""

import ConfigParser

__all__ = ["config", "__version__", "exceptions", "model", "controllers",
    "cbank", "upstreams"]

__version__ = "trunk"

config = ConfigParser.SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

# import common elements for convenience
from clusterbank.model import *
from clusterbank.controllers import *
from clusterbank.exceptions import *

# update __all__
import clusterbank.model
import clusterbank.controllers
import clusterbank.exceptions
__all__ = (__all__ + clusterbank.model.__all__ +
    clusterbank.controllers.__all__ + clusterbank.exceptions.__all__)

# monkeypatch entity properties that require access to the session
User.projects = projects_property(User.projects)
User.admin_projects = projects_property(User.admin_projects)
Project.members = users_property(Project.members)
Project.admins = users_property(Project.admins)

