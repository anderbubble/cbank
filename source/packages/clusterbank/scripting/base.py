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

from sqlalchemy import exceptions

from clusterbank.model import \
    Project, Resource, Request, Allocation, Lien, Charge
import clusterbank.upstream

def verify_configured ():
    if clusterbank.model.metadata.bind is None:
        raise Exception("database is not properly configured")
    for entity in ("User", "Project", "Resource"):
        if entity not in dir(clusterbank.upstream):
            raise Exception("upstream is not properly configured")


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
        try:
            return Allocation.query.filter(Allocation.id==value).one()
        except exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown allocation: %r" % (opt, value))
    
    def check_charge (self, opt, value):
        """Return a charge from its id."""
        try:
            return Charge.query.filter(Charge.id==value).one()
        except exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown charge: %r" % (opt, value))
    
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
        try:
            return Lien.query.filter(Lien.id==value).one()
        except exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown lien: %r" % (opt, value))
    
    def check_liens (self, opt, value):
        """Return a list of liens from a comma-separated list of ids."""
        return [self.check_lien(opt, id) for id in value.split(",")]
    
    def check_permissions (self, opt, value):
        """Verify a comma-separated list of permissions."""
        all_permissions = ("request", "allocate", "lien", "charge", "refund")
        if value == "all":
            return all_permissions
        else:
            permissions = value.split(",")
            for permission in permissions:
                if permission not in all_permissions:
                    raise optparse.OptionValueError(
                        "option %s: unknown permission: %r" % (opt, permission))
        return permissions
    
    def check_project (self, opt, value):
        """Return a project from its name."""
        try:
            return Project.by_name(value)
        except Project.DoesNotExist:
            raise optparse.OptionValueError(
                "option %s: unknown project: %r" % (opt, value))
    
    def check_request (self, opt, value):
        """Return a request from its id."""
        try:
            return Request.query.filter(Request.id==value).one()
        except exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown request: %r" % (opt, value))
    
    def check_resource (self, opt, value):
        """Return a resource from its name."""
        try:
            return Resource.by_name(value)
        except Resource.DoesNotExist:
            raise optparse.OptionValueError(
                "option %s: unknown resource: %r" % (opt, value))
    
    TYPES = (
        "resource", "project", "permissions", "date",
        "request", "allocation", "lien", "liens", "charge",
    ) + optparse.Option.TYPES
    
    TYPE_CHECKER = dict(
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


class ArgumentException (Exception):
    """Base class for errors in scripts."""

class MissingArgument (ArgumentException):
    """An expected argument was not specified."""

class InvalidArgument (ArgumentException):
    """The specified argument is not valid."""

class ExtraArguments (ArgumentException):
    """Unexpected arguments were present."""


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

    
    def get (self, option=None, values=None, **kwargs):
        
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
            raise MissingArgument("%s: error: missing argument: %s" % (self.prog, kwargs.get("arg", "unknown")))
        # Try to validate the argument using an option.
        if option is not None:
            try:
                value = option.TYPE_CHECKER[option.type](option, None, value)
            except optparse.OptionValueError:
                self.args.insert(0, value)
                raise InvalidArgument("%s: error: invalid argument: %s" %
                    (self.prog, value))
        return value
    
    def verify_empty (self):
        if len(self.args) != 0:
            raise ExtraArguments("%s: error: unexpected argument(s): %s" % (
                self.prog,
                ", ".join(self.args),
            ))
