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
            user_id = getpwnam(user_string)[2]
        except KeyError:
            user_id = user_string
    else:
        user_id = getpwuid(uid)[2]
    return str(user_id)


def user_out (id_):
    try:
        uid = int(id_)
    except ValueError:
        user_display = id_
    else:
        try:
            user_display = getpwuid(uid)[0]
        except KeyError:
            user_display = id_
    return str(user_display)


def group_in (group_string):
    try:
        gid = int(group_string)
    except ValueError:
        try:
            group_id = getgrnam(group_string)[2]
        except KeyError:
            group_id = group_string
    else:
        group_id = getgrgid(gid)[2]
    return str(group_id)


def group_out (id_):
    try:
        gid = int(id_)
    except ValueError:
        group_display = id_
    else:
        try:
            group_display = getgrgid(gid)[0]
        except KeyError:
            group_display = id_
    return str(group_display)


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
