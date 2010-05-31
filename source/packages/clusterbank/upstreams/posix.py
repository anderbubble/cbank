"""POSIX upstream plugin module """

from pwd import getpwnam, getpwuid, getpwall
from grp import getgrnam, getgrgid, getgrall

__all__ = [
    "user_in", "user_out",
    "project_in", "project_out",
    "resource_in", "resource_out",
    "project_member", "project_manager",
]


def user_in (user_string):
    try:
        uid = int(user_string)
    except ValueError:
        try:
            return getpwnam(user_string)[2]
        except KeyError:
            return str(user_string)
    else:
        return getpwuid(uid)[2]


def user_out (id_):
    try:
        uid = int(id_)
    except ValueError:
        return str(id_)
    else:
        try:
            return getpwuid(uid)[0]
        except KeyError:
            return str(id_)


def group_in (group_string):
    try:
        gid = int(group_string)
    except ValueError:
        try:
            return getgrnam(group_string)[2]
        except KeyError:
            return str(group_string)
    else:
        return getgrgid(gid)[2]


def group_out (id_):
    try:
        gid = int(id_)
    except ValueError:
        return str(id_)
    else:
        try:
            return getgrgid(gid)[0]
        except KeyError:
            return str(id_)


resource_in = project_in = group_in
resource_out = project_out = group_out


def project_member (project_id, user_id):
    if project_manager(project_id, user_id):
        return True
    try:
        gid = int(project_id)
        uid = int(user_id)
    except ValueError:
        return False
    try:
        users = getgrgid(gid)[3]
    except KeyError:
        return False

    for user_string in getgrgid(gid)[3]:
        if uid == user_in(user_string):
            return True
    return False

def project_manager (project_id, user_id):
    try:
        gid = int(project_id)
        uid = int(user_id)
    except ValueError:
        return False
    try:
        user_gid = getpwuid(uid)[3]
    except KeyError:
        return False

    if user_gid == gid:
        return True
