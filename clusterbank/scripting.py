from optparse import Option, OptionValueError
from datetime import datetime
import time

from clusterbank.models import \
    User, Resource, Project, \
    Request, Allocation, Lien, Charge

class CBOption (Option):
    
    PRIVILEGES = ("request", "allocate", "lien", "charge", "refund")
    
    def check_user (self, opt, value):
        try:
            return User.from_upstream_name(value)
        except User.DoesNotExist:
            raise OptionValueError(
                "option %s: unknown user: %r" % (opt, value))
    
    def check_resource (self, opt, value):
        try:
            return Resource.from_upstream_name(value)
        except Resource.DoesNotExist:
            raise OptionValueError(
                "option %s: unknown resource: %r" % (opt, value))
    
    def check_project (self, opt, value):
        project = Project.from_upstream_name(value)
        if project:
            return project
        else:
            raise OptionValueError(
                "option %s: unknown project: %r" % (opt, value))
    
    def check_date (self, opt, value):
        try:
            return datetime(*time.strptime(value, "%Y-%m-%d")[0:6])
        except ValueError:
            raise OptionValueError(
                "option %s: invalid date: %r" % (opt, value))
    
    def check_privileges (self, opt, value):
        if value == "all":
            privileges = self.PRIVILEGES
        else:
            privileges = value.split(",")
            for privilege in privileges:
                if privilege not in self.PRIVILEGES:
                    raise OptionValueError(
                        "option %s: unknown privilege: %r" % (opt, privilege))
        return privileges
    
    def check_request (self, opt, value):
        try:
            return Request.get_by(id=value)
        except Request.DoesNotExist:
            raise OptionValueError(
                "option %s: unknown request: %r" % (opt, value))
    
    def check_allocation (self, opt, value):
        try:
            return Allocation.get_by(id=value)
        except Allocation.DoesNotExist:
            raise OptionValueError(
                "option %s: unknown allocation: %r" % (opt, value))
    
    def check_lien (self, opt, value):
        try:
            return Lien.get_by(id=value)
        except Lien.DoesNotExist:
            raise OptionValueError(
                "option %s: unknown lien: %r" % (opt, value))
    
    def check_liens (self, opt, value):
        return [self.check_lien(opt, id) for id in value.split(",")]
    
    def check_charge (self, opt, value):
        try:
            return Charge.get_by(id=value)
        except Charge.DoesNotExist:
            raise OptionValueError(
                "option %s: unknown charge: %r" % (opt, value))
    
    TYPES = Option.TYPES + (
        "user", "resource", "project", "privileges", "date",
        "request", "allocation", "lien", "liens", "charge")
    
    TYPE_CHECKER = Option.TYPE_CHECKER.copy()
    TYPE_CHECKER['user'] = check_user
    TYPE_CHECKER['resource'] = check_resource
    TYPE_CHECKER['project'] = check_project
    TYPE_CHECKER['privileges'] = check_privileges
    TYPE_CHECKER['date'] = check_date
    TYPE_CHECKER['request'] = check_request
    TYPE_CHECKER['allocation'] = check_allocation
    TYPE_CHECKER['lien'] = check_lien
    TYPE_CHECKER['liens'] = check_liens
    TYPE_CHECKER['charge'] = check_charge
