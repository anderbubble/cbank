"""example upstream plugin module

Classes:
Project -- upstream project
Resource -- upstream resource
User -- upstream user
"""


__all__ = [
    "get_project_id", "get_project_name",
    "get_project_members", "get_project_admins",
    "get_resource_id", "get_resource_name",
    "get_user_id", "get_user_name",
    "get_member_projects", "get_admin_projects",
]


class Entity (object):
    
    """Generic entities.
    
    Attributes:
    id -- the entity id
    name -- the entity name
    """
    
    def __init__ (self, id_, name):
        """Initialize a new entity.
        
        Arguments:
        id -- the entity id
        name -- the entity name
        """
        self.id = id_
        self.name = name


class User (Entity):
    
    """User entities."""


class Project (Entity):
    
    """Project entities."""
    
    def __init__ (self, id_, name):
        """Initialize a new project.
        
        Arguments:
        id -- the project id
        name -- the project name
        """
        Entity.__init__(self, id_, name)
        self.members = []
        self.admins = []


class Resource (Entity):
    
    """Resource entities."""


def get_user_id (name):
    """Given a user name, return the user id."""
    for user in users:
        if user.name == name:
            return user.id
    return None


def get_user_name (id_):
    """Given a user id, return the user name."""
    for user in users:
        if user.id == id_:
            return user.name
    return None


def get_project_id (name):
    """Given a project name, return the project id, or None."""
    for project in projects:
        if project.name == name:
            return project.id
    return None


def get_project_name (id_):
    """Given a project id, return the project name or None."""
    for project in projects:
        if project.id == id_:
            return project.name
    return None


def get_resource_id (name):
    """Given a resource name, return the resource id."""
    for resource in resources:
        if resource.name == name:
            return resource.id
    return None


def get_resource_name (id_):
    """Given a resource id, return the resource name."""
    for resource in resources:
        if resource.id == id_:
            return resource.name
    return None


def get_project_members (id_):
    """Given a project id, return the ids of the project's members."""
    for project in projects:
        if project.id == id_:
            return [user.id for user in project.members]
    return []


def get_project_admins (id_):
    """Given a project id, return the ids of the project's admins."""
    for project in projects:
        if project.id == id_:
            return [user.id for user in project.admins]
    return []


def get_member_projects (id_):
    """Given a user id, return the ids of the user's projects."""
    return [project.id for project in projects
        if [user for user in project.members
            if user.id == id_]]


def get_admin_projects (id_):
    """Given a user id, return the ids of the projects the user admins."""
    return [project.id for project in projects
        if [user for user in project.admins
            if user.id == id_]]


users = []
projects = []
resources = []

