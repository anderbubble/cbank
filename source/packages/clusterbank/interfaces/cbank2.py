"""cbank part deux"""

import sys
import os
import pwd

from clusterbank.model import User, Allocation

class UnknownUser (Exception): pass

def handle_exceptions (func, *args, **kwargs):
    try:
        func(*args, **kwargs)
    except KeyboardInterrupt:
        sys.exit(1)
    except UnknownUser, e:
        print >> sys.stderr, e
        sys.exit(1)

def main ():    
    user = get_current_user()
    allocations = Allocation.query.filter(Allocation.project.in_(user.projects))
    display_allocations(allocations)

def get_current_user ():
    uid = os.getuid()
    try:
        passwd_entry = pwd.getpwuid(uid)
    except KeyError:
        raise UnknownUser("Unable to determine the current user.")
    username = passwd_entry[0]
    user = User.by_name(username)
    if not user:
        raise UnknownUser("User '%s' was not found." % username)
    return user

if __name__ == "__main__":
    handle_exceptions(main)
