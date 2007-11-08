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
    Project, Resource, Request, Allocation, Hold, Charge
import clusterbank.upstream

def verify_configured ():
    if clusterbank.model.metadata.bind is None:
        raise Exception("database is not properly configured")
    for entity in ("Project", "Resource"):
        if entity not in dir(clusterbank.upstream):
            raise Exception("upstream is not properly configured")


class Option (optparse.Option):
    """Extension of optparse.Options for clusterbank parsing.
    
    Methods:
    having -- Apply attributes to the option.
    check_allocation -- Return an allocation from its id.
    check_charge -- Return a charge from its id.
    check_date -- Return a datetime from YYYY-MM-DD.
    check_hold -- Return a hold from its id.
    check_holds -- Return a list of holds from a comma-separated list of ids.
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
    
    def check_hold (self, opt, value):
        """Return a hold from its id."""
        try:
            return Hold.query.filter(Hold.id==value).one()
        except exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown hold: %r" % (opt, value))
    
    def check_holds (self, opt, value):
        """Return a list of holds from a comma-separated list of ids."""
        return [self.check_hold(opt, id) for id in value.split(",")]
    
    def check_permissions (self, opt, value):
        """Verify a comma-separated list of permissions."""
        all_permissions = ("request", "allocate", "hold", "charge", "refund")
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
        "request", "allocation", "hold", "holds", "charge",
    ) + optparse.Option.TYPES
    
    TYPE_CHECKER = dict(
        resource = check_resource,
        project = check_project,
        permissions = check_permissions,
        date = check_date,
        request = check_request,
        allocation = check_allocation,
        hold = check_hold,
        holds = check_holds,
        charge = check_charge,
        **optparse.Option.TYPE_CHECKER
    )
