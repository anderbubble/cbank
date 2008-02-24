"""example upstream plugin module

Classes:
Project -- upstream project
Resource -- upstream resource

Exceptions:
NotFound -- requested resource not found
"""

from sqlalchemy import MetaData, Table, Column, ForeignKey
import sqlalchemy.types as types
# types.TEXT was renamed types.Text in SA 0.4.3
try:
    types.Text
except AttributeError:
    types.Text = types.TEXT
import sqlalchemy.exceptions as exceptions
from sqlalchemy.orm import sessionmaker, scoped_session


__all__ = ["NotFound", "Project", "Resource"]


class NotFound (Exception):
    """The specified entity does not exist."""


class UpstreamEntity (object):
    
    """Superclass for entities in the upstream model.
    
    Class methods:
    by_id -- Retrieve an entity by canonical identifier.
    by_name -- Retrieve an entity by name.
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
    
    def __repr__ (self):
        return "<%s %r>" % (self.__class__.__name__, self.id)
    
    def __str__ (self):
        if self.name is not None:
            return str(self.name)
        else:
            return repr(self)
    
    @classmethod
    def by_id (cls, id):
        """Retrieve an entity by identifier.
        
        Arguments:
        id -- Canonical, immutable, integer identifier.
        """
        try:
            return cls.query.filter_by(id=id).one()
        except exceptions.InvalidRequestError:
            raise NotFound(id)
    
    @classmethod
    def by_name (cls, name):
        """Retrieve an entity by name.
        
        Arguments:
        name -- Canonical string identifier.
        """
        try:
            return cls.query.filter_by(name=name).one()
        except exceptions.InvalidRequestError:
            raise NotFound(name)


class Project (UpstreamEntity):
    
    """Upstream project.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string id.
    
    Class methods:
    by_id -- Retrieve a project by identifier.
    by_name -- Retrieve a project by name.
    """


class Resource (UpstreamEntity):
    
    """Upstream resource.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string identifier.
    
    Class methods:
    by_id -- Retrieve a resource by identifier.
    by_name -- Retrieve a resource by name.
    """


metadata = MetaData()

projects_table = Table("projects", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("name", types.Text, nullable=False, unique=True),
)

resource_types_table = Table("resource_types", metadata,
    Column("id", types.Integer, nullable=False, primary_key=True),
    Column("name", types.Text, nullable=False, unique=True),
)

Session = scoped_session(sessionmaker(autoflush=True, transactional=True))

Session.mapper(Project, projects_table, properties=dict(
    id = projects_table.c.id,
    name = projects_table.c.name,
))

Session.mapper(Resource, resource_types_table, properties=dict(
    id = resource_types_table.c.id,
    name = resource_types_table.c.name,
))
