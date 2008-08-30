"""example upstream plugin module

Classes:
Project -- upstream project
Resource -- upstream resource
User -- upstream user
"""

import ConfigParser

from sqlalchemy import MetaData, Table, Column, ForeignKey, create_engine
import sqlalchemy.types as types
import sqlalchemy.exceptions as exceptions
from sqlalchemy.orm import sessionmaker, scoped_session, mapper, relation

__all__ = [
    "get_project_id", "get_project_name",
    "get_project_members", "get_project_owners",
    "get_resource_id", "get_resource_name",
    "get_user_id", "get_user_name",
    "get_member_projects", "get_owner_projects",
]

def _get_entity_id (cls, name):
    s = Session()
    try:
        return s.query(cls).filter_by(name=name).one().id
    except exceptions.InvalidRequestError:
        return None

def _get_entity_name (cls, id):
    s = Session()
    try:
        return s.query(cls).filter_by(id=id).one().name
    except exceptions.InvalidRequestError:
        return None

def get_user_id (name):
    return _get_entity_id(User, name)

def get_user_name (id):
    return _get_entity_name(User, id)

def get_member_projects (id):
    s = Session()
    try:
        user = s.query(User).filter_by(id=id).one()
    except exceptions.InvalidRequestError:
        return []
    else:
        return [project.id for project in user.projects]

def get_owner_projects (id):
    s = Session()
    try:
        user = s.query(User).filter_by(id=id).one()
    except exceptions.InvalidRequestError:
        return []
    else:
        return [project.id for project in user.projects_owned]

def get_project_id (name):
    return _get_entity_id(Project, name)

def get_project_name (id):
    return _get_entity_name(Project, id)

def get_project_members (id):
    s = Session()
    try:
        project = s.query(Project).filter_by(id=id).one()
    except exceptions.InvalidRequestError:
        return []
    else:
        return [user.id for user in project.members]

def get_project_owners (id):
    s = Session()
    try:
        project = s.query(Project).filter_by(id=id).one()
    except exceptions.InvalidRequestError:
        return []
    else:
        return [user.id for user in project.owners]

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
    
    def __init__ (self, **kwargs):
        UpstreamEntity.__init__(self, **kwargs)
        self.projects = kwargs.get("projects", [])
        self.projects_owned = kwargs.get("projects_owned", [])


class Project (UpstreamEntity):
    
    """Upstream project.
    
    Attributes:
    id -- canonical, immutable integer identifier
    name -- canonical string identifier
    """
    
    def __init__ (self, **kwargs):
        UpstreamEntity.__init__(self, **kwargs)
        self.members = kwargs.get("members", [])
        self.owners = kwargs.get("owners", [])


class Resource (UpstreamEntity):
    
    """Upstream resource.
    
    Attributes:
    id -- canonical, immutable integer identifier
    name -- canonical string identifier
    """


metadata = MetaData()

users = Table("users", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("name", types.Text, nullable=False, unique=True),
)

projects = Table("projects", metadata,
    Column("id", types.Integer, primary_key=True),
    Column("name", types.Text, nullable=False, unique=True),
)

projects_members = Table("projects_members", metadata,
    Column("project_id", None, ForeignKey("projects.id"), primary_key=True),
    Column("user_id", None, ForeignKey("users.id"), primary_key=True),
)

projects_owners = Table("projects_owners", metadata,
    Column("project_id", None, ForeignKey("projects.id"), primary_key=True),
    Column("user_id", None, ForeignKey("users.id"), primary_key=True),
)

resources = Table("resources", metadata,
    Column("id", types.Integer, nullable=False, primary_key=True),
    Column("name", types.Text, nullable=False, unique=True),
)

Session = scoped_session(sessionmaker(autoflush=True, transactional=True))

mapper(User, users, properties=dict(
    id = users.c.id,
    name = users.c.name,
))

mapper(Project, projects, properties=dict(
    id = projects.c.id,
    name = projects.c.name,
    members = relation(User, secondary=projects_members, backref="projects"),
    owners = relation(User, secondary=projects_owners, backref="projects_owned"),
))

mapper(Resource, resources, properties=dict(
    id = resources.c.id,
    name = resources.c.name,
))

def configure ():
    config = ConfigParser.SafeConfigParser()
    config.read(["/etc/clusterbank.conf"])
    uri = config.get("upstream", "database")
    metadata.bind = create_engine(uri)
    Session.bind = metadata.bind

try:
    configure()
except:
    pass
