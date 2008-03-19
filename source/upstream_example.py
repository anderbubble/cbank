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
from sqlalchemy.orm import sessionmaker, scoped_session, relation


__all__ = [
    "get_project_id", "get_project_name",
    "get_resource_id", "get_resource_name",
]

def _get_entity_id (cls, name):
    try:
        return cls.query.filter_by(name=name).one().id
    except exceptions.InvalidRequestError:
        return None

def _get_entity_name (cls, id):
    try:
        return cls.query.filter_by(id=id).one().name
    except exceptions.InvalidRequestError:
        return None

def get_user_id (name):
    return _get_entity_id(User, name)

def get_user_name (id):
    return _get_entity_name(User, id)

def get_project_id (name):
    return _get_entity_id(Project, name)

def get_project_name (id):
    return _get_entity_name(Project, id)

def get_project_members (id):
    return [user.id for user in User.query.join("projects").filter_by(id=id)]

def get_user_projects (id):
    return [project.id for project in Project.query.join("users").filter_by(id=id)]

def get_project_owners (id):
    return [user.id for user in User.query.join("projects").filter_by(id=id).filter(ProjectUserRelation.owner==True)]

def get_resource_id (name):
    return _get_entity_id(Resource, name)

def get_resource_name (id):
    return _get_entity_name(Resource, id)


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


class User (UpstreamEntity):
    
    """Upstream user.
    
    Attributes:
    id -- canonical, immutable integer identifier
    name -- canonical string identifier
    """


class Project (UpstreamEntity):
    
    """Upstream project.
    
    Attributes:
    id -- canonical, immutable integer identifier
    name -- canonical string identifier
    """


class Resource (UpstreamEntity):
    
    """Upstream resource.
    
    Attributes:
    id -- canonical, immutable integer identifier
    name -- canonical string identifier
    """


class ProjectUserRelationship (object):
    
    """A relationship between a user and a project."""
    
    def __init__ (self, **kwargs):
        self.project = kwargs.get("project")
        self.user = kwargs.get("user")
        self.owner = kwargs.get("owner", False)


metadata = MetaData()

users_table = Table("users", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("name", types.Text, nullable=False, unique=True),
)

projects_table = Table("projects", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("name", types.Text, nullable=False, unique=True),
)

projects_users_table = Table("projects_users", metadata,
    Column("user_id", None, ForeignKey("users.id"), primary_key=True),
    Column("project_id", None, ForeignKey("projects.id"), primary_key=True),
    Column("owner", types.Boolean, nullable=False, default=False),
)

resources_table = Table("resources", metadata,
    Column("id", types.Integer, nullable=False, primary_key=True),
    Column("name", types.Text, nullable=False, unique=True),
)

Session = scoped_session(sessionmaker(autoflush=True, transactional=True))

Session.mapper(User, users_table, properties=dict(
    id = users_table.c.id,
    name=users_table.c.name,
))

Session.mapper(Project, projects_table, properties=dict(
    id = projects_table.c.id,
    name = projects_table.c.name,
    users = relation(User, secondary=projects_users_table, backref="projects"),
))

Session.mapper(Resource, resources_table, properties=dict(
    id = resources_table.c.id,
    name = resources_table.c.name,
))

Session.mapper(ProjectUserRelationship, projects_users_table, properties=dict(
    project = relation(Project),
    user = relation(User),
    owner = projects_users_table.c.owner,
))
