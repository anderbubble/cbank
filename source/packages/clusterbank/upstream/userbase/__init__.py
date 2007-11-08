"""Userbase upstream module.

The userbase module uses SQLAlchemy to build an interface over
the MCS userbase database.
"""

from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError
import warnings

from sqlalchemy import create_engine, exceptions
from sqlalchemy.orm import scoped_session, sessionmaker, relation

from clusterbank.upstream.userbase.metadata import \
    metadata, projects_table, resource_types_table
from clusterbank.upstream.userbase.model import Project, Resource
    
__all__ = [
    "Project", "Resource",
]

Session = scoped_session(sessionmaker(transactional=True))

Session.mapper(Project, projects_table, properties=dict(
    id = projects_table.c.project_id,
    name = projects_table.c.project_name,
))

Session.mapper(Resource, resource_types_table, properties=dict(
    id = resource_types_table.c.resource_id,
    name = resource_types_table.c.resource_name,
))


config = SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    uri = config.get("userbase", "database")
except (NoSectionError, NoOptionError):
    warnings.warn("no userbase database configured", UserWarning)
else:
    try:
        metadata.bind = create_engine(uri)
    except Exception, e:
        warnings.warn("unable to connect to %s (%s)" % (uri, e), UserWarning)
