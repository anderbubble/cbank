"""cbank part deux"""

import sys
import os
import pwd
from itertools import izip
import string

from clusterbank.model import User, Project, Allocation

class UnknownUser (Exception): pass

def main ():
    handle_exceptions(run)

def handle_exceptions (func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except KeyboardInterrupt:
        sys.exit(1)
    except UnknownUser, e:
        print >> sys.stderr, e
        sys.exit(1)

def run ():
    user = get_current_user()
    project_ids = [project.id for project in user.projects]
    allocations = Allocation.query.join("project").filter(Project.id.in_(project_ids))
    display_allocations(allocations)

def display_allocations (allocations):
    format = Formatter([15, 15, (10, string.rjust), (10, string.rjust), (10, string.rjust)])
    print format(["Resource", "Project", "Total", "Charged", "Available"])
    print format(["-"*15, "-"*15, "-"*10, "-"*10, "-"*10])
    for allocation in allocations:
        print format([allocation.resource, allocation.project, allocation.amount, allocation.amount_charged, allocation.amount_available])

class Formatter (object):
    
    def __init__ (self, cols, sep=" "):
        self.cols = [self._with_alignment(col) for col in cols]
        self.sep = sep
    
    @staticmethod
    def _with_alignment (col):
        try:
            width, alignment = col
        except TypeError:
            width = col
            alignment = string.ljust
        return (width, alignment)
    
    def __call__ (self, *args, **kwargs):
        return self.format(*args, **kwargs)
    
    def format (self, args):
        assert len(args) == len(self.cols), "Too many arguments to format."
        return self.sep.join([align(str(arg), width) for (arg, (width, align)) in izip(args, self.cols)])

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
