"""example in-memory upstream plugin module"""


__all__ = [
    "user_in", "user_out",
    "project_in", "project_out",
    "resource_in", "resource_out",
    "project_members", "project_managers",
]


def user_in (name_or_id):
    """Given a user name, return the user id."""
    for user in users:
        if str(user.id) == str(name_or_id):
            return user.id
    for user in users:
        if str(user.name) == str(name_or_id):
            return user.id


def user_out (id_):
    """Given a user id, return the user name."""
    for user in users:
        if str(user.id) == str(id_):
            return user.name


def project_in (name_or_id):
    """Given a project name, return the project id, or None."""
    for project in projects:
        if str(project.name) == str(name_or_id):
            return project.id
    for project in projects:
        if str(project.id) == str(name_or_id):
            return project.id


def project_out (id_):
    """Given a project id, return the project name or None."""
    for project in projects:
        if str(project.id) == id_:
            return project.name


def resource_in (name_or_id):
    """Given a resource name or id, return the resource id."""
    for resource in resources:
        if str(resource.id) == str(name_or_id):
            return resource.id
    for resource in resources:
        if str(resource.name) == str(name_or_id):
            return resource.id


def resource_out (id_):
    """Given a resource id, return the resource name."""
    for resource in resources:
        if str(resource.id) == str(id_):
            return resource.name


def project_member (project_id, user_id):
    """Given a project id and a user id, return true or false."""
    for project in projects:
        if project.id == project_id:
            return user_id in [user.id for user in project.members]
    return False


def project_manager (project_id, user_id):
    """Given a project id and a user id, return true or false."""
    for project in projects:
        if project.id == project_id:
            return user_id in [user.id for user in project.managers]
    return False


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
        self.managers = []


class Resource (Entity):
    
    """Resource entities."""


users = []
projects = []
resources = []
