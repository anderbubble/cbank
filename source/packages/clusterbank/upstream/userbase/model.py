"""Userbase model for userbase plugin.

Classes:
UpstreamEntity -- Base class for upstream entities.
User -- Upstream user.
Project -- Upstream project.
Resource -- Upstream resource.
"""

from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import create_session, mapper, relation
from sqlalchemy.ext.sessioncontext import SessionContext
from sqlalchemy.ext.assignmapper import assign_mapper


__all__ = [
    "metadata", "session", "User", "Project", "Resource",
]

metadata = MetaData()

user_table = Table("user", metadata,
    Column("userbase_id", Integer, nullable=False, primary_key=True),
    Column("username", String, nullable=False, unique=True),
)

projects_table = Table("projects", metadata,
    Column("project_id", Integer, primary_key=True),
    Column("project_name", String, nullable=False, unique=True),
)

project_members_table = Table("project_members", metadata,
    Column("userbase_id", Integer, ForeignKey("user.userbase_id"), primary_key=True),
    Column("project_id", Integer, ForeignKey("projects.project_id"), primary_key=True),
)

resource_types_table = Table("resource_types", metadata,
    Column("resource_id", Integer, nullable=False, primary_key=True),
    Column("resource_name", String, nullable=False, unique=True),
)


class UpstreamEntity (object):
    """Superclass for entities in the upstream model.
    
    Class methods:
    by_id -- Retrieve an entity by canonical identifier.
    by_name -- Retrieve an entity by name.
    
    Exceptions:
    DoesNotExist -- The specified entity does not exist.
    """
    
    
    class DoesNotExist (Exception):
        """The specified entity does not exist."""
        
        label = "entity"
        
        def __str__ (self):
            try:
                return "%s %i does not exist" % (self.label, self.message)
            except TypeError:
                return '%s "%s" does not exist' % (self.label, self.message)
    
    
    def __init__ (self, id=None, name=None):
        self.id = id
        self.name = name
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def __str__ (self):
        return self.name or "?"
    
    @classmethod
    def by_id (cls, id):
        """Retrieve an entity by identifier.
        
        Arguments:
        id -- Canonical, immutable, integer identifier.
        """
        entity = cls.get_by(id=id)
        if not entity:
            raise cls.DoesNotExist(id)
        return entity
    
    @classmethod
    def by_name (cls, name):
        """Retrieve an entity by name.
        
        Arguments:
        name -- Canonical string identifier.
        """
        entity = cls.get_by(name=name)
        if not entity:
            raise cls.DoesNotExist(name)
        return entity


class User (UpstreamEntity):
    """Upstream user.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string identifier.
    projects -- Projects the user is a member of.
    
    Class methods:
    by_id -- Retrieve a user by identifier.
    by_name -- Retrieve a user by name.
    
    Exceptions:
    DoesNotExist -- The specified user does not exist.
    """
    
    class DoesNotExist (UpstreamEntity.DoesNotExist):
        """The specified user does not exist."""
        
        label = "user"


class Project (UpstreamEntity):
    """Upstream project.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string id.
    users -- Users that are members of the project.
    
    Class methods:
    by_id -- Retrieve a project by identifier.
    by_name -- Retrieve a project by name.
    
    Exceptions:
    DoesNotExist -- The specified project does not exist.
    """
    
    class DoesNotExist (UpstreamEntity.DoesNotExist):
        """The specified project does not exist."""
        
        label = "project"


class Resource (UpstreamEntity):
    """Upstream resource.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string identifier.
    
    Class methods:
    by_id -- Retrieve a resource by identifier.
    by_name -- Retrieve a resource by name.
    
    Exceptions:
    DoesNotExist -- The specified resource does not exist.
    """
    
    class DoesNotExist (UpstreamEntity.DoesNotExist):
        """The specified project does not exist."""
        
        label = "resource"


context = SessionContext(create_session)

assign_mapper(context, User, user_table, properties=dict(
    id = user_table.c.userbase_id,
    name = user_table.c.username,
    projects = relation(Project, secondary=project_members_table),
))

assign_mapper(context, Project, projects_table, properties=dict(
    id = projects_table.c.project_id,
    name = projects_table.c.project_name,
    users = relation(User, secondary=project_members_table),
))

assign_mapper(context, Resource, resource_types_table, properties=dict(
    id = resource_types_table.c.resource_id,
    name = resource_types_table.c.resource_name,
))
