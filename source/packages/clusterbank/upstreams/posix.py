"""POSIX upstream plugin module """

from pwd import getpwnam, getpwuid, getpwall
from grp import getgrnam, getgrgid, getgrall

__all__ = [
    "get_project_id", "get_project_name",
    "get_project_members", "get_project_owners",
    "get_resource_id", "get_resource_name",
    "get_user_id", "get_user_name",
    "get_member_projects", "get_owner_projects",
]

def get_project_id (name):
    try:
        return getgrnam(name)[2]
    except KeyError:
        return None

def get_project_name (id):
    try:
        return getgrgid(id)[0]
    except KeyError:
        return None

def get_project_members (id):
    try:
        return [getpwnam(name)[2] for name in getgrgid(id)[3]]
    except KeyError:
        return []

def get_project_owners (id):
    try:
        return [user[2] for user in getpwall() if user[3] == getgrgid(id)[0]]
    except KeyError:
        return []

def get_member_projects (id):
    try:
        return [group[2] for group in getgrall() if getpwuid(id)[0] in group[3]]
    except KeyError:
        return []

def get_owner_projects (id):
    return [getgrgid(getpwuid(id)[3])[2]]

def get_resource_id (name):
    return get_project_id(name)

def get_resource_name (id):
    return get_project_name(id)

def get_user_id (name):
    try:
        return getpwnam(name)[2]
    except KeyError:
        return None

def get_user_name (id):
    try:
        return getpwuid(id)[0]
    except KeyError:
        return None

