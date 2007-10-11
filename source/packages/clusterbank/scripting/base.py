"""Scripting operations for clusterbank.

Classes:
OptionParser -- Declarative extenxion of optparse.OptionParser.
Option -- Extension of optparse.Option.
"""

import sys
import os
from datetime import datetime
import time
import optparse

from clusterbank.models import \
    fetch_user, fetch_project, fetch_resource, User, Project, Resource, Request, Allocation, Lien, Charge


PERMISSIONS = ("request", "allocate", "lien", "charge", "refund")


class ScriptingError (Exception):
    """Base class for errors in scripts."""
    STATUS = -1

class MissingArgument (ScriptingError):
    """A required argument was not specified."""
    STATUS = -2

class InvalidArgument (ScriptingError):
    """The specified argument does not exist, or represents an invalid value."""
    STATUS = -3

class ExtraArguments (ScriptingError):
    """Unexpected arguments were present."""
    STATUS = -4

class NotPermitted (ScriptingError):
    """The specified action is not permitted."""
    STATUS = -5

class OptionParser (optparse.OptionParser):
    """Extension of optparse.Options for declarative definition."""
    
    def __init__ (self, *args, **kwargs):
        attributes = dict()
        for attribute in ("version", "usage", "description"):
            try:
                value = getattr(self, "__%s__" % attribute)
            except AttributeError:
                pass
            else:
                attributes[attribute] = value
        attributes.update(**kwargs)
        #super(OptionParser, self).__init__(*args, **attributes) # doesn't work because OptionParser is an old-style class
        optparse.OptionParser.__init__(self, *args, **attributes)
        try:
            defaults = getattr(self, "__defaults__")
        except AttributeError:
            pass
        else:
            self.set_defaults(**defaults)


class Option (optparse.Option):
    """Extension of optparse.Options for clusterbank parsing.
    
    Methods:
    having -- Apply attributes to the option.
    check_allocation -- Return an allocation from its id.
    check_charge -- Return a charge from its id.
    check_date -- Return a datetime from YYYY-MM-DD.
    check_lien -- Return a lien from its id.
    check_liens -- Return a list of liens from a comma-separated list of ids.
    check_permissions -- Verify a comma-separated list of permissions.
    check_project -- Return a project from its name.
    check_request -- Return a request from its id.
    check_resource -- Return a resource from its name.
    check_user -- Return a user from its name.
    """
    
    def having (self, **kwargs):
        """Apply attributes to the option."""
        for (key, value) in kwargs.items():
            setattr(self, key, value)
        return self
    
    def check_allocation (self, opt, value):
        """Return an allocation from its id."""
        allocation = Allocation.get_by(id=value)
        if not allocation:
            raise optparse.OptionValueError(
                "option %s: unknown allocation: %r" % (opt, value))
        return allocation
    
    def check_charge (self, opt, value):
        """Return a charge from its id."""
        charge = Charge.get_by(id=value)
        if not charge:
            raise optparse.OptionValueError(
                "option %s: unknown charge: %r" % (opt, value))
        return charge
    
    def check_date (self, opt, value):
        """Return a datetime from YYYY-MM-DD."""
        format = "%Y-%m-%d" # YYYY-MM-DD
        try:
            # return datetime.strptime(value, format) # requires Python >= 2.5
            return datetime(*time.strptime(value, format)[0:6]) # backwards compatible
        except ValueError:
            raise optparse.OptionValueError(
                "option %s: invalid date: %r" % (opt, value))
    
    def check_lien (self, opt, value):
        """Return a lien from its id."""
        lien = Lien.get_by(id=value)
        if not lien:
            raise optparse.OptionValueError(
                "option %s: unknown lien: %r" % (opt, value))
        return lien
    
    def check_liens (self, opt, value):
        """Return a list of liens from a comma-separated list of ids."""
        return [self.check_lien(opt, id) for id in value.split(",")]
    
    def check_permissions (self, opt, value):
        """Verify a comma-separated list of permissions."""
        if value == "all":
            permissions = PERMISSIONS
        else:
            permissions = value.split(",")
            for permission in permissions:
                if permission not in PERMISSIONS:
                    raise optparse.OptionValueError(
                        "option %s: unknown permission: %r" % (opt, permission))
        return permissions
    
    def check_project (self, opt, value):
        """Return a project from its name."""
        try:
            return fetch_project(value)
        except Project.DoesNotExist:
            raise optparse.OptionValueError(
                "option %s: unknown project: %r" % (opt, value))
    
    def check_request (self, opt, value):
        """Return a request from its id."""
        request = Request.get_by(id=value)
        if not request:
            raise optparse.OptionValueError(
                "option %s: unknown request: %r" % (opt, value))
        return request
    
    def check_resource (self, opt, value):
        """Return a resource from its name."""
        try:
            return fetch_resource(value)
        except Resource.DoesNotExist:
            raise optparse.OptionValueError(
                "option %s: unknown resource: %r" % (opt, value))
    
    def check_user (self, opt, value):
        """Return a user from its name."""
        try:
            return fetch_user(value)
        except User.DoesNotExist:
            raise optparse.OptionValueError(
                "option %s: unknown user: %r" % (opt, value))
    
    TYPES = (
        "user", "resource", "project", "permissions", "date",
        "request", "allocation", "lien", "liens", "charge",
    ) + optparse.Option.TYPES
    
    TYPE_CHECKER = dict(
        user = check_user,
        resource = check_resource,
        project = check_project,
        permissions = check_permissions,
        date = check_date,
        request = check_request,
        allocation = check_allocation,
        lien = check_lien,
        liens = check_liens,
        charge = check_charge,
        **optparse.Option.TYPE_CHECKER
    )


OPTIONS = dict(
    
    allocation = Option("-a", "--allocation",
        dest = "allocation",
        type = "allocation",
        action = "store",
        metavar="ALLOCATION",
    ),
    
    user = Option("-u", "--user",
        dest = "user",
        type = "user",
        action = "store",
        metavar = "USER",
    ),
    
    grant = Option("-g", "--grant",
        dest = "grant",
        type = "permissions",
        action = "store",
        metavar = "PERMISSIONS",
    ),
    
    revoke = Option("-r", "--revoke",
        dest = "revoke",
        type = "permissions",
        action = "store",
        metavar="PERMISSIONS",
    ),
    
    list = Option("-l", "--list",
        dest = "list",
        action = "store_true",
    ),
    
    lien = Option("-n", "--lien",
        dest = "lien",
        type = "lien",
        action = "store",
        metavar = "LIEN",
    ),
    
    liens = Option("-n", "--liens",
        dest = "liens",
        type = "liens",
        action = "store",
        metavar="LIENS",
    ),
    
    project = Option("-p", "--project",
        dest = "project",
        type = "project",
        action = "store",
        metavar = "PROJECT",
    ),
    
    resource = Option("-r", "--resource",
        dest = "resource",
        type = "resource",
        action = "store",
        metavar = "RESOURCE",
    ),
    
    request = Option("-q", "--request",
        dest = "request",
        type = "request",
        action = "store",
        metavar = "REQUEST",
    ),
    
    time = Option("-t", "--time",
        dest = "time",
        type = "int",
        action = "store",
        metavar = "TIME",
    ),
    
    credit = Option("-c", "--credit-limit",
        dest = "credit",
        type = "int",
        action = "store",
        metavar = "LIMIT",
    ),
    
    start = Option("-s", "--start",
        dest = "start",
        type = "date",
        action = "store",
        metavar = "DATE",
    ),
    
    expiration = Option("-x", "--expiration",
        dest = "expiration",
        type = "date",
        action = "store",
        metavar = "DATE",
    ),
    
    explanation = Option("-e", "--explanation",
       dest = "explanation",
       type = "string",
       action = "store",
       metavar = "NOTES",
    ),
    
    charge = Option("-c", "--charge",
        dest = "charge",
        type = "charge",
        action = "store",
        metavar = "CHARGE",
    ),
    
)

class ArgumentParser (object):
    
    """Parse arguments sequentially.
    
    Attributes:
    args -- Arguments to parse.
    
    Methods:
    get -- Get an argument from the list.
    """
    
    class NotEmpty (Exception):
        """The argument list is not empty."""
    
    class NoValue (Exception):
        """The value could not be found in the arguments or options."""
    
    class InvalidArgument (Exception):
        """The argument used did not pass option validation."""
    
    def __init__ (self, args=sys.argv[1:], prog=os.path.basename(sys.argv[0])):
        """Initialize a new ArgumentParser
        
        Arguments:
        args -- Arguments to be parsed. (default sys.argv[1:])
        prog -- Command-line program run. (default os.path.basename(sys.argv[0]))
        """
        self.args = args[:]
        self.prog = prog

    
    def get (self, option=None, values=None):
        
        """Get the next argument.
        
        If the value has already been parsed as an option, return that
        value in stead.
        
        Keyword arguments:
        option -- An option that can be used to validate the argument.
        values -- Previously parsed option values.
        """
        
        # Try to use a previously parsed option.
        try:
            value = getattr(values, option.dest)
        except AttributeError:
            pass
        else:
            if value is not None:
                return value
        
        # Get a value from the argument queue.
        try:
            value = self.args.pop(0)
        except IndexError:
            raise self.NoValue("%s: error: missing argument" % self.prog)
        # Try to validate the argument using an option.
        try:
            value = option.TYPE_CHECKER[option.type](option, None, value)
        except AttributeError:
            pass
        except optparse.OptionValueError:
            self.args.insert(0, value)
            raise self.InvalidArgument("%s: error: invalid argument: %s" %
                (self.prog, value))
        return value
    
    def verify_empty (self):
        if len(self.args) != 0:
            raise self.NotEmpty("%s: error: unexpected argument(s): %s" % (
                self.prog,
                ", ".join(self.args),
            ))
