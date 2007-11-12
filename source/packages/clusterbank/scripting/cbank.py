"""clusterbank cli.

Classes:
Option -- extension of optparse.Option

Functions:
verify_configured -- ensure that the library is properly configured
"""

import sys
import os
from datetime import datetime
import time
import optparse
from optparse import OptionParser

from sqlalchemy import exceptions

import clusterbank
import clusterbank.model
from clusterbank.model import \
    Project, Resource, Request, Allocation, Hold, Charge, Refund
import clusterbank.upstream


class Option (optparse.Option):
    """Extension of optparse.Options for clusterbank parsing.
    
    Methods:
    check_allocation -- return an allocation from its id
    check_charge -- return a charge from its id
    check_date -- return a datetime from YYYY-MM-DD
    check_hold -- return a hold from its id
    check_holds -- return a list of holds from a comma-separated list of ids
    check_permissions -- verify a comma-separated list of permissions
    check_project -- return a project from its name
    check_request -- return a request from its id
    check_resource -- return a resource from its name
    """
    
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
    
    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER.update(dict(
        resource = check_resource,
        project = check_project,
        permissions = check_permissions,
        date = check_date,
        request = check_request,
        allocation = check_allocation,
        hold = check_hold,
        holds = check_holds,
        charge = check_charge,
    ))


def verify_configured ():
    if clusterbank.model.metadata.bind is None:
        raise NotConfigured("database")
    for entity in ("Project", "Resource"):
        if entity not in dir(clusterbank.upstream):
            raise NotConfigured("upstream")

parser = OptionParser(
    version = clusterbank.__version__,
    usage = "%prog <directive> [options]",
)
parser.add_option(Option("-l", "--list",
    help="list active instances",
    dest="list", action="store_true"))
parser.add_option(Option("-p", "--project",
    help="request amount for or list requests for PROJECT", metavar="PROJECT",
    dest="project", type="project", action="store"))
parser.add_option(Option("-r", "--resource",
    help="request amount of or list requests for RESOURCE", metavar="RESOURCE",
    dest="resource", type="resource", action="store"))
parser.add_option(Option("-t", "--amount",
    help="request AMOUNT", metavar="AMOUNT",
    dest="amount", type="int", action="store"))
parser.add_option(Option("-s", "--start",
    help="begin on DATE", metavar="DATE",
    dest="start", type="date", action="store"))
parser.add_option(Option("-m", "--comment",
    help="misc. NOTES", metavar="NOTES",
    dest="comment", type="string", action="store"))
parser.add_option(Option("-q", "--request",
    help="specify REQUEST id", metavar = "REQUEST",
    dest="request", type="request", action="store"))
parser.add_option(Option("-a", "--allocation",
    help="specify ALLOCATION id", metavar="ALLOCATION",
    dest="allocation", type="allocation", action="store"))
parser.add_option(Option("-d", "--hold",
    help="specify HOLD id", metavar="HOLD",
    dest="hold", type="hold", action="store"))
parser.add_option(Option("-x", "--expiration",
    help="expire on DATE", metavar="DATE",
    dest="expiration", type="date", action="store"))
parser.add_option(Option("-c", "--charge",
    help="specify CHARGE id", metavar="CHARGE",
    dest="charge", type="charge", action="store"))
parser.set_defaults(list=False)


class UnknownDirective (Exception):
    
    def __init__ (self, directive):
        self.directive = directive
    
    def __str__ (self):
        return "cbank: unknown directive: %s" % self.directive


class UnexpectedArguments (Exception):
    
    def __init__ (self, arguments):
        self.arguments = arguments
    
    def __str__ (self):
        return "unknown argument(s): %s" % ", ".join(self.arguments)


class MissingOption (Exception):
    
    def __init__ (self, option):
        self.option = option
    
    def __str__ (self):
        return "must specify %s" % self.option


class NotConfigured (Exception):
    
    def __str__ (self):
        return "not configured"


def main (argv=None):
    if argv is None:
        argv = sys.argv
    parser.prog = os.path.basename(argv[0])
    
    verify_configured()
    options, args = parser.parse_args(args=argv[1:])
    try:
        directive = args.pop(0)
    except IndexError:
        raise UnknownDirective("not specified")
    if args:
        raise UnexpectedArguments(args)
    
    if directive == "request":
        if options.list:
            return request_list(project=options.project, resource=options.resource)
        else:
            for option in ("amount", ):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            request = Request(project=options.project, resource=options.resource, amount=options.amount, start=options.start, comment=options.comment)
            clusterbank.model.Session.commit()
            return [request]
    elif directive == "allocation":
        if options.list:
            return allocation_list(project=options.project, resource=options.resource)
        else:
            for option in ("project", "resource", "amount", "start", "expiration"):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            allocation = Allocation(project=options.project, resource=options.resource, start=options.start, expiration=options.expiration, amount=options.amount, comment=options.comment)
            if options.request is not None and options.request.allocation is None:
                options.request.allocation = allocation
            clusterbank.model.Session.commit()
            return [allocation]
    elif directive == "hold":
        if options.list:
            return hold_list()
        elif options.allocation is not None:
            for option in ("amount", ):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            hold = Hold(allocation=options.allocation, amount=options.amount, comment=options.comment)
            clusterbank.model.Session.commit()
            return [hold]
        else:
            for option in ("amount", ):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            if options.project is None or options.resource is None:
                raise MissingOption("allocation or project and resource")
            allocations = allocation_list(request=options.request, project=options.project, resource=options.resource)
            holds = Hold.distributed(allocations, amount=options.amount, comment=options.comment)
            clusterbank.model.Session.commit()
            return holds
    elif directive == "charge":
        if options.list:
            return charge_list(allocation=options.allocation, request=options.request, project=options.project, resource=options.resource)
        elif options.allocation is not None:
            for option in ("amount", ):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            charge = Charge(allocation=options.allocation, amount=options.amount, comment=options.comment)
            clusterbank.model.Session.commit()
            return [charge]
        else:
            for option in ("amount", ):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            if options.project is None or options.resource is None:
                raise MissingOption("allocation or project and resource")
            allocations = allocation_list(project=options.project, resource=options.resource)
            charges = Charge.distributed(allocations, amount=options.amount, comment=options.comment)
            clusterbank.model.Session.commit()
            return charges
    elif directive == "refund":
        if options.list:
            return refund_list(charge=options.charge, hold=options.hold, allocation=options.allocation, request=options.request, project=options.project, resource=options.resource)
        else:
            for option in ("charge", "amount"):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            refund = Refund(charge=options.charge, amount=options.amount, comment=options.comment)
            clusterbank.model.Session.commit()
            return [refund]
    else:
        raise UnknownDirective(directive)

def request_list (**kwargs):
    requests = Request.query.filter(Request.allocation==None)
    if kwargs.get("project"):
        requests = requests.filter(Request.project==kwargs['project'])
    if kwargs.get("resource"):
        requests = requests.filter(Request.resource==kwargs['resource'])
    return requests

def allocation_list (**kwargs):
    now = datetime.now()
    allocations = Allocation.query.filter(Allocation.start<=now)
    allocations = allocations.filter(Allocation.expiration>now)
    if kwargs.get("project") is not None:
        allocations = allocations.filter(Allocation.project==kwargs.get("project"))
    if kwargs.get("resource") is not None:
        allocations = allocations.filter(Allocation.resource==kwargs.get("resource"))
    return allocations

def hold_list (**kwargs):
    now = datetime.now()
    holds = Hold.query()
    if kwargs.get("allocation") is not None:
        holds = holds.filter(Hold.allocation==kwargs.get("allocation"))
    else:
        holds = holds.filter(Allocation.start<=now)
        holds = holds.filter(Allocation.expiration>now)
    if kwargs.get("project") is not None:
        holds = holds.filter(Request.project == kwargs.get("project"))
    if kwargs.get("resource") is not None:
        holds = holds.filter(Request.resource == kwargs.get("resource"))
    return holds

def charge_list (**kwargs):
    now = datetime.now()
    charges = Charge.query()
    if kwargs.get("allocation") is not None:
        charges = charges.filter(Charge.allocation==kwargs.get("allocation"))
    else:
        charges = charges.filter(Allocation.start<=now)
        charges = charges.filter(Allocation.expiration>now)
    if kwargs.get("project") is not None:
        charges = charges.filter(Allocation.project==kwargs.get("project"))
    if kwargs.get("resource") is not None:
        charges = charges.filter(Allocation.resource==kwargs.get("resource"))
    return charges

def refund_list (**kwargs):
    now = datetime.now()
    refunds = Refund.query()
    if kwargs.get("charge") is not None:
        refunds = refunds.filter(Refund.charge==kwargs.get("charge"))
    if kwargs.get("allocation") is not None:
        refunds = refunds.filter(Charge.allocation==kwargs.get("allocation"))
    else:
        refunds = refunds.filter(Allocation.start<=now)
        refunds = refunds.filter(Allocation.expiration>now)
    if kwargs.get("project") is not None:
        refunds = refunds.filter(Allocation.project==kwargs.get("project"))
    if kwargs.get("resource") is not None:
        refunds = refunds.filter(Allocation.resource==kwargs.get("resource"))
    return refunds
