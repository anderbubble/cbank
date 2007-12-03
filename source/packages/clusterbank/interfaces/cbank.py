#!/usr/bin/env python

"""clusterbank cli.

Classes:
Option -- extension of optparse.Option

Exceptions:
UnknownDirective -- unknown directive specified
UnexpectedArguments -- extra unparsed arguments
MissingOption -- a required option was not specified
NotConfigured -- the library is not fully configured

Functions:
main -- main (argv-parsing) function
verify_configured -- ensure that the library is properly configured
request_list -- get existing requests
allocation_list -- get existing allocations
hold_list -- get existing holds
charge_list -- get existing charges
refund_list -- get existing refunds

Objects:
parser -- cbank option parser (instance of optparser.OptionParser)
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
    Session, Project, Resource, Request, Allocation, Hold, Charge, Refund
import clusterbank.upstream

__all__ = [
    "Option", "parser",
    "main", "verify_configured", "request_list", "allocation_list",
    "hold_list", "charge_list", "refund_list",
    "UnknownDirective", "UnexpectedArguments", "MissingOption",
    "NotConfigured",
]


class Option (optparse.Option):
    """Extension of optparse.Options for clusterbank parsing.
    
    Methods:
    check_project -- return a project from its name
    check_resource -- return a resource from its name
    check_date -- return a datetime from YYYY-MM-DD
    check_allocation -- return an allocation from its id
    check_charge -- return a charge from its id
    check_hold -- return a hold from its id
    check_request -- return a request from its id
    """
    
    def check_date (self, opt, value):
        """Return a datetime from YYYY-MM-DD."""
        format = "%Y-%m-%d" # YYYY-MM-DD
        try:
            # return datetime.strptime(value, format) # requires Python >= 2.5
            return datetime(*time.strptime(value, format)[0:6]) # backwards compatible
        except ValueError:
            raise optparse.OptionValueError(
                "option %s: invalid date: %r" % (opt, value))
    
    def check_project (self, opt, value):
        """Return a project from its name."""
        try:
            return Project.by_name(value)
        except Project.DoesNotExist:
            raise optparse.OptionValueError(
                "option %s: unknown project: %r" % (opt, value))
    
    def check_resource (self, opt, value):
        """Return a resource from its name."""
        try:
            return Resource.by_name(value)
        except Resource.DoesNotExist:
            raise optparse.OptionValueError(
                "option %s: unknown resource: %r" % (opt, value))
    
    def check_request (self, opt, value):
        """Return a request from its id."""
        try:
            return Request.query.filter(Request.id==value).one()
        except exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown request: %r" % (opt, value))
    
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
    
    def check_hold (self, opt, value):
        """Return a hold from its id."""
        try:
            return Hold.query.filter(Hold.id==value).one()
        except exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown hold: %r" % (opt, value))
    
    def check_refund (self, opt, value):
        """Return a refund from its id."""
        try:
            return Refund.query.filter(Refund.id==value).one()
        except exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "options %s: unknown refund: %r" % (opt, value))
    
    TYPES = optparse.Option.TYPES + (
        "date",
        "resource", "project",
        "request", "allocation", "hold", "charge", "refund",
    )
    
    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER.update(dict(
        resource = check_resource,
        project = check_project,
        date = check_date,
        request = check_request,
        allocation = check_allocation,
        hold = check_hold,
        charge = check_charge,
        refund = check_refund,
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
parser.add_option(Option("-f", "--refund",
    help="specify REFUND id", metavar="REFUND",
    dest="refund", type="refund", action="store"))
parser.set_defaults(list=False)


class UnknownDirective (Exception):
    
    """An unknown directive was specified."""
    
    def __init__ (self, directive):
        self.directive = directive
    
    def __str__ (self):
        return "cbank: unknown directive: %s" % self.directive


class UnexpectedArguments (Exception):
    
    """Extra unparsed arguments."""
    
    def __init__ (self, arguments):
        self.arguments = arguments
    
    def __str__ (self):
        return "unknown argument(s): %s" % ", ".join(self.arguments)


class MissingOption (Exception):
    
    """A required option was not specified."""
    
    def __init__ (self, option):
        self.option = option
    
    def __str__ (self):
        return "must specify %s" % self.option


class InvalidOption (Exception):
    
    """An invalid option was specified."""
    
    def __init__ (self, option):
        self.option = option
    
    def __str__ (self):
        return "cannot specify %s" % self.option


class NotConfigured (Exception):
    
    """The library is not configured."""
    
    def __str__ (self):
        return "not configured"


def parse_directive (directive):
    directives = ("request", "allocation", "allocate", "hold", "release", "charge", "refund")
    matches = [each for each in directives if each.startswith(directive)]
    if len(matches) == 1:
        return matches[0]
    elif set(matches) == set(["allocation", "allocate"]):
        return "allocation"
    else:
        return directive

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
    else:
        directive = parse_directive(directive)
    if args:
        raise UnexpectedArguments(args)
    
    if directive == "request":
        if options.list:
            if options.request is not None:
                return [options.request]
            else:
                return request_list(project=options.project, resource=options.resource)
        else:
            for option in ("project", "resource", "amount"):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            request = Request(project=options.project, resource=options.resource, amount=options.amount, start=options.start, comment=options.comment)
            Session.commit()
            return [request]
    elif directive == "allocation":
        if options.list:
            if options.allocation is not None:
                return [options.allocation]
            else:
                return allocation_list(project=options.project, resource=options.resource, request=options.request)
        else:
            for option in ("project", "resource", "amount", "start", "expiration"):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            if options.request:
                requests = [options.request]
            else:
                requests = []
            allocation = Allocation(project=options.project, resource=options.resource, requests=requests, start=options.start, expiration=options.expiration, amount=options.amount, comment=options.comment)
            Session.commit()
            return [allocation]
    elif directive == "hold":
        if options.list:
            if options.hold is not None:
                return [options.hold]
            else:
                return hold_list(project=options.project, resource=options.resource, request=options.request, allocation=options.allocation)
        elif options.allocation is not None:
            for option in ("amount", ):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            hold = Hold(allocation=options.allocation, amount=options.amount, comment=options.comment)
            Session.commit()
            return [hold]
        else:
            for option in ("amount", ):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            if options.project is None or options.resource is None:
                raise MissingOption("allocation or project and resource")
            if options.allocation is not None:
                allocations = [options.allocation]
            else:
                allocations = allocation_list(project=options.project, resource=options.resource, request=options.request)
            holds = Hold.distributed(allocations, amount=options.amount, comment=options.comment)
            Session.commit()
            return holds
    elif directive == "release":
        if options.list:
            raise InvalidOption("list")
        if options.hold is not None:
            holds = [options.hold]
        else:
            holds = hold_list(project=options.project, resource=options.resource, request=options.request, allocation=options.allocation)
            holds = list(holds)
        for hold in holds:
            hold.active = False
        Session.commit()
        return holds
    elif directive == "charge":
        if options.list:
            if options.charge is not None:
                return [options.charge]
            else:
                return charge_list(project=options.project, resource=options.resource, request=options.request, allocation=options.allocation)
        elif options.allocation is not None:
            for option in ("amount", ):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            charge = Charge(allocation=options.allocation, amount=options.amount, comment=options.comment)
            Session.commit()
            return [charge]
        else:
            for option in ("amount", ):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            if options.project is None or options.resource is None:
                raise MissingOption("allocation or project and resource")
            if options.allocation is not None:
                allocations = [options.allocation]
            else:
                allocations = allocation_list(project=options.project, resource=options.resource, request=options.request)
            charges = Charge.distributed(allocations, amount=options.amount, comment=options.comment)
            Session.commit()
            return charges
    elif directive == "refund":
        if options.list:
            if options.refund is not None:
                return [options.refund]
            else:
                return refund_list(project=options.project, resource=options.resource, request=options.request, allocation=options.allocation, charge=options.charge)
        else:
            for option in ("charge", "amount"):
                if getattr(options, option, None) is None:
                    raise MissingOption(option)
            refund = Refund(charge=options.charge, amount=options.amount, comment=options.comment)
            Session.commit()
            return [refund]
    else:
        raise UnknownDirective(directive)

def console_main (argv=None, **kwargs):
    stderr = kwargs.get("stderr") or sys.stderr
    try:
        entities = main(argv)
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception, e:
        print >> stderr, e
        sys.exit(1)
    
    for entity in entities:
        print entity

def request_list (**kwargs):
    """Get existing requests.
    
    Keyword arguments:
    project -- project the request is for
    resource -- resource the request is for
    """
    requests = Request.query.filter(~Request.allocations.any())
    if kwargs.get("project"):
        requests = requests.filter(Request.project==kwargs['project'])
    if kwargs.get("resource"):
        requests = requests.filter(Request.resource==kwargs['resource'])
    return requests

def allocation_list (**kwargs):
    """Get active allocations.
    
    Keyword arguments:
    project -- project the allocation is for
    resource -- resource the allocation is for
    request -- request answered by the allocation
    """
    now = datetime.now()
    allocations = Allocation.query.filter(Allocation.start<=now)
    allocations = allocations.filter(Allocation.expiration>now)
    if kwargs.get("project") is not None:
        allocations = allocations.filter(Allocation.project==kwargs.get("project"))
    if kwargs.get("resource") is not None:
        allocations = allocations.filter(Allocation.resource==kwargs.get("resource"))
    if kwargs.get("request") is not None:
        allocations = allocations.filter(Allocation.requests.contains(kwargs.get("request")))
    return allocations

def hold_list (**kwargs):
    """Get holds on active allocations.
    
    Keyword arguments:
    project -- project of the hold's allocation
    resource -- resource of the hold's allocation
    request -- holds on allocations for a request
    allocation -- holds on a specific allocaiton
    """
    holds = Hold.query.filter(Hold.active==True)
    if kwargs.get("project") is not None:
        holds = holds.filter(Allocation.project==kwargs.get("project"))
    if kwargs.get("resource") is not None:
        holds = holds.filter(Allocation.resource==kwargs.get("resource"))
    if kwargs.get("request") is not None:
        holds = holds.filter(Allocation.requests.contains(kwargs.get("request")))
    if kwargs.get("allocation") is not None:
        holds = holds.filter(Hold.allocation==kwargs.get("allocation"))
    else:
        now = datetime.now()
        holds = holds.filter(Allocation.start<=now)
        holds = holds.filter(Allocation.expiration>now)
    return holds

def charge_list (**kwargs):
    """Get charges on active allocations.
    
    Keyword arguments:
    project -- project of the charge's allocation
    resource -- resource of the charge's allocation
    request -- related request
    allocation -- charges on a specific allocaiton
    """
    charges = Charge.query()
    if kwargs.get("project") is not None:
        charges = charges.filter(Allocation.project==kwargs.get("project"))
    if kwargs.get("resource") is not None:
        charges = charges.filter(Allocation.resource==kwargs.get("resource"))
    if kwargs.get("request") is not None:
        charges = charges.filter(Allocation.requests.contains(kwargs.get("request")))
    if kwargs.get("allocation") is not None:
        charges = charges.filter(Charge.allocation==kwargs.get("allocation"))
    else:
        now = datetime.now()
        charges = charges.filter(Allocation.start<=now)
        charges = charges.filter(Allocation.expiration>now)
    return charges

def refund_list (**kwargs):
    """Get refunds to active allocations.
    
    Keyword arguments:
    project -- project of the refund's allocation
    resource -- resource of the refund's allocation
    request -- related request
    allocation -- refunds to a specific allocaiton
    charge -- refunds to a specific charge
    """
    refunds = Refund.query()
    if kwargs.get("project") is not None:
        refunds = refunds.filter(Allocation.project==kwargs.get("project"))
    if kwargs.get("resource") is not None:
        refunds = refunds.filter(Allocation.resource==kwargs.get("resource"))
    if kwargs.get("request") is not None:
        refunds = refunds.filter(Allocation.requests.contains(kwargs.get("request")))
    if kwargs.get("allocation") is not None:
        refunds = refunds.filter(Charge.allocation==kwargs.get("allocation"))
    else:
        now = datetime.now()
        refunds = refunds.filter(Allocation.start<=now)
        refunds = refunds.filter(Allocation.expiration>now)
    if kwargs.get("charge") is not None:
        refunds = refunds.filter(Refund.charge==kwargs.get("charge"))
    return refunds

if __name__ == "__main__":
    console_main()
