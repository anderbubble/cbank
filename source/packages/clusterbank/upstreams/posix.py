"""POSIX upstream plugin module """

from pwd import getpwnam, getpwuid, getpwall
from grp import getgrnam, getgrgid, getgrall

__all__ = [
    "get_project_id", "get_project_name",
    "get_project_members", "get_project_admins",
    "get_resource_id", "get_resource_name",
    "get_user_id", "get_user_name",
    "get_member_projects", "get_admin_projects",
]

def get_project_id (name):
    """Given a group name, return the gid, or None."""
    try:
        return getgrnam(name)[2]
    except KeyError:
        return None

def get_project_name (gid):
    """Given a gid, return the group name, or None."""
    try:
        return getgrgid(int(gid))[0]
    except KeyError:
        return None

def get_project_members (gid):
    """Given a gid, return the group member uids."""
    try:
        return [str(getpwnam(name)[2]) for name in getgrgid(int(gid))[3]]
    except KeyError:
        return []

def get_project_admins (gid):
    """Given a gid, return the uids of users with that default group."""
    try:
        return [user[2] for user in getpwall() if user[3] == getgrgid(int(gid))[0]]
    except KeyError:
        return []

def get_member_projects (uid):
    """Given a uid, return the gids of that user's projects."""
    try:
        return [str(group[2]) for group in getgrall()
            if getpwuid(int(uid))[0] in group[3]]
    except KeyError:
        return []

def get_admin_projects (uid):
    """Given a uid, return a list containing that user's default group."""
    return [str(getgrgid(getpwuid(int(uid))[3])[2])]

def get_resource_id (name):
    """Given a group name, return the gid, or None."""
    return get_project_id(name)

def get_resource_name (gid):
    """Given a gid, return the group name, or None."""
    return get_project_name(gid)

def get_user_id (username):
    """Given a username, return the uid, or None."""
    try:
        return getpwnam(username)[2]
    except KeyError:
        return None

def get_user_name (uid):
    """Given a uid, return the username, or None."""
    try:
        return getpwuid(int(uid))[0]
    except KeyError:
        return None
