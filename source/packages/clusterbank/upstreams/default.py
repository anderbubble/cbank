"""example upstream plugin module

Classes:
Project -- upstream project
Resource -- upstream resource
User -- upstream user
"""

__all__ = [
    "get_project_id", "get_project_name",
    "get_project_members", "get_project_owners",
    "get_resource_id", "get_resource_name",
    "get_user_id", "get_user_name",
    "get_member_projects", "get_owner_projects",
]

class Entity (object):
    
    def __init__ (self, id, name):
        self.id = id
        self.name = name


class User (Entity): pass


class Project (Entity):
    
    def __init__ (self, id, name):
        Entity.__init__(self, id, name)
        self.members = []
        self.owners = []


class Resource (Entity): pass

users = []
projects = []
resources = []

def get_project_id (name):
    for project in projects:
        if project.name == name:
            return project.id
    return None

def get_project_name (id):
    for project in projects:
        if project.id == id:
            return project.name
    return None

def get_project_members (id):
    for project in projects:
        if project.id == id:
            return [user.id for user in project.members]
    return []

def get_project_owners (id):
    for project in projects:
        if project.id == id:
            return [user.id for user in project.owners]
    return []

def get_member_projects (id):
    return [project.id for project in projects
        if [user for user in project.members
            if user.id==id]]

def get_owner_projects (id):
    return [project.id for project in projects
        if [user for user in project.owners
            if user.id==id]]

def get_resource_id (name):
    for resource in resources:
        if resource.name == name:
            return resource.id
    return None

def get_resource_name (id):
    for resource in resources:
        if resource.id == id:
            return resource.name
    return None

def get_user_id (name):
    for user in users:
        if user.name == name:
            return user.id
    return None

def get_user_name (id):
    for user in users:
        if user.id == id:
            return user.name
    return None

