#!/usr/bin/env python

"""clusterbank cli.

Classes:
Option -- extension of optparse.Option

Exceptions:
UnknownDirective -- unknown directive specified
UnexpectedArguments -- extra unparsed arguments
MissingOption -- a required option was not specified

Functions:
main -- main (argv-parsing) function
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
import pwd
from datetime import datetime
import time
import optparse
from ConfigParser import SafeConfigParser as ConfigParser, NoSectionError, NoOptionError
from optparse import OptionParser

from sqlalchemy import and_
import sqlalchemy.exceptions

import clusterbank
import clusterbank.model
import clusterbank.exceptions as exceptions
from clusterbank.model import \
    Session, User, Project, Resource, Request, Allocation, Hold, Charge, Refund
import clusterbank.upstream

__all__ = [
    "Option", "parser", "config",
    "main", "request_list", "allocation_list",
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
        except exceptions.NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown project: %r" % (opt, value))
    
    def check_resource (self, opt, value):
        """Return a resource from its name."""
        try:
            return Resource.by_name(value)
        except exceptions.NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown resource: %r" % (opt, value))
    
    def check_user (self, opt, value):
        """Return a user from its name."""
        try:
            return User.by_name(value)
        except exceptions.NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown user: %r" % (opt, value))
    
    def check_request (self, opt, value):
        """Return a request from its id."""
        try:
            return Request.query.filter_by(id=value).one()
        except sqlalchemy.exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown request: %r" % (opt, value))
    
    def check_allocation (self, opt, value):
        """Return an allocation from its id."""
        try:
            return Allocation.query.filter_by(id=value).one()
        except sqlalchemy.exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown allocation: %r" % (opt, value))
    
    def check_charge (self, opt, value):
        """Return a charge from its id."""
        try:
            return Charge.query.filter_by(id=value).one()
        except sqlalchemy.exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown charge: %r" % (opt, value))
    
    def check_hold (self, opt, value):
        """Return a hold from its id."""
        try:
            return Hold.query.filter_by(id=value).one()
        except sqlalchemy.exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown hold: %r" % (opt, value))
    
    def check_refund (self, opt, value):
        """Return a refund from its id."""
        try:
            return Refund.query.filter_by(id=value).one()
        except sqlalchemy.exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "options %s: unknown refund: %r" % (opt, value))
    
    TYPES = optparse.Option.TYPES + (
        "date",
        "resource", "project", "user",
        "request", "allocation", "hold", "charge", "refund",
    )
    
    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER.update(dict(
        resource = check_resource,
        project = check_project,
        user = check_user,
        date = check_date,
        request = check_request,
        allocation = check_allocation,
        hold = check_hold,
        charge = check_charge,
        refund = check_refund,
    ))

config = ConfigParser()
config.read(["/etc/clusterbank.conf"])

current_user = pwd.getpwuid(os.getuid())[0]

def require_admin (user=None):
    if user is None:
        user = current_user
    try:
        admins = config.get("cbank", "admins")
    except (NoSectionError, NoOptionError):
        raise NotPermitted()
    admins = admins.split(",")
    if not user in admins:
        raise NotPermitted()

parser = OptionParser(
    version = clusterbank.__version__,
    usage = os.linesep.join([
        "%prog [options] <directive>",
        "",
        "Directives:",
        "  request -- process allocation requests",
        "  allocation -- process allocations",
        "  hold -- process holds",
        "  charge -- process charges",
        "  refund -- process refunds",
    ])
)
parser.add_option(Option("-l", "--list",
    help="list active instances",
    dest="list", action="store_true"))
parser.add_option(Option("-p", "--project",
    help="specify a project by NAME", metavar="NAME",
    dest="project", type="project"))
parser.add_option(Option("-r", "--resource",
    help="specify a resource by NAME", metavar="NAME",
    dest="resource", type="resource"))
parser.add_option(Option("-u", "--user",
    help="specify a user by NAME", metavar="NAME",
    dest="user", type="user"))
parser.add_option(Option("-t", "--amount",
    help="specify an AMOUNT", metavar="AMOUNT",
    dest="amount", type="int"))
parser.add_option(Option("-s", "--start",
    help="begin on DATE", metavar="DATE",
    dest="start", type="date"))
parser.add_option(Option("-x", "--expiration",
    help="expire on DATE", metavar="DATE",
    dest="expiration", type="date"))
parser.add_option(Option("-m", "--comment",
    help="attach a COMMENT", metavar="COMMENT",
    dest="comment", type="string"))
parser.add_option(Option("-q", "--request",
    help="specify a request by ID", metavar = "ID",
    dest="request", type="request"))
parser.add_option(Option("-a", "--allocation",
    help="specify an allocation by ID", metavar="ID",
    dest="allocation", type="allocation"))
parser.add_option(Option("-d", "--hold",
    help="specify a hold by ID", metavar="ID",
    dest="hold", type="hold"))
parser.add_option(Option("-c", "--charge",
    help="specify a charge by ID", metavar="ID",
    dest="charge", type="charge"))
parser.add_option(Option("-f", "--refund",
    help="specify a refund by ID", metavar="ID",
    dest="refund", type="refund"))
parser.set_defaults(list=False)


class NotPermitted (Exception):
    
    """The specified action is not allowed."""
    
    def __init__ (self, action=None):
        self.action = action
    
    def __str__ (self):
        error = "cbank: not permitted"
        if self.action:
            return "%s: %s" % (error, self.action)
        else:
            return error


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


def parse_directive (directive):
    directives = ("request", "allocation", "allocate", "hold", "release", "charge", "refund")
    matches = [each for each in directives if each.startswith(directive)]
    if len(matches) == 1:
        return matches[0]
    elif set(matches) == set(["allocation", "allocate"]):
        return "allocation"
    else:
        return directive

def get_base_query (cls):
    if issubclass(cls, Request):
        return Request.query.outerjoin("resource").outerjoin("project").outerjoin(["allocations", "holds", "user"]).outerjoin(["allocations", "charges", "user"]).outerjoin(["allocations", "charges", "refunds"]).reset_joinpoint()
    elif issubclass(cls, Allocation):
        return Allocation.query.outerjoin("resource").outerjoin("project").outerjoin("requests").outerjoin(["holds", "user"]).outerjoin(["charges", "user"]).outerjoin(["charges", "refunds"]).reset_joinpoint()
    elif issubclass(cls, Hold):
        return Hold.query.filter_by(active=True).outerjoin("user").outerjoin(["allocation", "project"]).outerjoin(["allocation", "resource"]).outerjoin(["allocation", "requests"]).outerjoin(["allocation", "charges", "user"]).outerjoin(["allocation", "charges", "refunds"]).reset_joinpoint()
    elif issubclass(cls, Charge):
        return Charge.query.outerjoin("user").outerjoin(["allocation", "project"]).outerjoin(["allocation", "resource"]).outerjoin(["allocation", "requests"]).outerjoin(["allocation", "holds", "user"]).outerjoin("refunds").reset_joinpoint()
    elif issubclass(cls, Refund):
        return Refund.query.outerjoin(["charge", "user"]).outerjoin(["charge", "allocation", "project"]).outerjoin(["charge", "allocation", "resource"]).outerjoin(["charge", "allocation", "requests"]).outerjoin(["charge", "allocation", "holds", "user"]).reset_joinpoint()
    else:
        raise UnknownDirective(directive)

def main (argv=None):
    if argv is None:
        argv = sys.argv
    parser.prog = os.path.basename(argv[0])
    options, args = parser.parse_args(args=argv[1:])
    try:
        directive = args.pop(0)
    except IndexError:
        raise UnknownDirective("not specified")
    directive = parse_directive(directive)
    if args:
        raise UnexpectedArguments(args)
    
    assert (
        clusterbank.model.metadata.bind is not None
        and clusterbank.upstream is not None
    ), "clusterbank is not configured"
    
    if options.list:
        
        if directive == "request":
            query = get_base_query(Request)
            format = request_format
        elif directive == "allocation":
            query = get_base_query(Allocation)
            format = allocation_format
        elif directive == "hold":
            query = get_base_query(Hold)
            format = hold_format
        elif directive == "charge":
            query = get_base_query(Charge)
            format = charge_format
        elif directive == "refund":
            query = get_base_query(Refund)
            format = refund_format
        else:
            raise UnknownDirective("list: %s" % directive)
        
        query = filter_options(query, options)
        
        for each in query:
            print format(each)
    
    else:
        
        if directive == "request":
            require_options(["project", "resource", "amount"], options)
            request = Request(project=options.project, resource=options.resource, amount=options.amount, start=options.start, comment=options.comment)
            Session.commit()
            print request_format(request)
        
        elif directive == "allocation":
            require_admin()
            require_options(["project", "resource", "amount", "expiration"], options)
            if options.request:
                requests = [options.request]
            else:
                requests = []
            allocation = Allocation(project=options.project, resource=options.resource, requests=requests, start=options.start or datetime.now(), expiration=options.expiration, amount=options.amount, comment=options.comment)
            Session.commit()
            print allocation_format(allocation)
        
        elif directive == "hold":
            require_admin()
            require_options(["amount"], options)
            try:
                require_options(["allocation"], options)
            except MissingOption, e:
                try:
                    require_options(["project", "resource"], options)
                except MissingOption:
                    raise e
                else:
                    allocations = filter_options(get_base_query(Allocation), options)
                    holds = Hold.distributed(allocations, user=options.user, amount=options.amount, comment=options.comment)
            else:
                holds = [Hold(allocation=options.allocation, user=options.user, amount=options.amount, comment=options.comment)]
            Session.commit()
            for hold in holds:
                print hold_format(hold)
            
        elif directive == "release":
            require_admin()
            holds = filter_options(get_base_query(Hold), options)
            holds = list(holds) # remember which holds were active
            for hold in holds:
                hold.active = False
            Session.commit()
            for hold in holds:
                print hold_format(hold)
        
        elif directive == "charge":
            require_admin()
            require_options(["amount"], options)
            try:
                require_options(["allocation"], options)
            except MissingOption, e:
                try:
                    require_options(["project", "resource"], options)
                except MissingOption:
                    raise e
                else:
                    allocations = filter_options(get_base_query(Allocation), options)
                    charges = Charge.distributed(allocations, user=options.user, amount=options.amount, comment=options.comment)
            else:
                charges = [Charge(allocation=options.allocation, user=options.user, amount=options.amount, comment=options.comment)]
            Session.commit()
            for charge in charges:
                print charge_format(charge)
        
        elif directive == "refund":
            require_admin()
            require_options(["charge", "amount"], options)
            refund = Refund(charge=options.charge, amount=options.amount, comment=options.comment)
            Session.commit()
            print refund_format(refund)
        
        else:
            raise UnknownDirective(directive)

def console_main (argv=None, **kwargs):
    stderr = kwargs.get("stderr", sys.stderr)
    try:
        main(argv)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception, e:
        print >> stderr, e
        sys.exit(1)

def require_options (option_list, options):
    for option in option_list:
        if getattr(options, option, None) is None:
            raise MissingOption(option)

def filter_options (query, options):
    if options.project:
        query = query.filter(Project.id==options.project.id)
    if options.resource:
        query = query.filter(Resource.id==options.resource.id)
    if options.request:
        query = query.filter(Request.id==options.request.id)
    if options.allocation:
        query = query.filter(Allocation.id==options.allocation.id)
    if options.hold:
        query = query.filter(Hold.id==options.hold.id)
    if options.charge:
        query = query.filter(Charge.id==options.charge.id)
    if options.refund:
        query = query.filter(Refund.id==options.refund.id)
    if options.user:
        query = query.filter(User.id==options.user.id)
    return query

def request_format (request):
    id = str(request.id).ljust(6)
    project = str(request.project).ljust(15)
    resource = str(request.resource).ljust(10)
    amount = str(request.amount)
    return " ".join([id, project, resource, amount])

def allocation_format (allocation):
    id = str(allocation.id).ljust(6)
    project = str(allocation.project).ljust(15)
    resource = str(allocation.resource).ljust(10)
    amount = "%i/%i" % (allocation.amount - allocation.amount_charged, allocation.amount)
    return " ".join([id, project, resource, amount])

def hold_format (hold):
    id = str(hold.id).ljust(6)
    allocation_id = str(hold.allocation.id).ljust(6)
    project = str(hold.allocation.project).ljust(15)
    resource = str(hold.allocation.resource).ljust(10)
    amount = str(hold.amount)
    return " ".join([id, allocation_id, project, resource, amount])

def charge_format (charge):
    id = str(charge.id).ljust(6)
    allocation_id = str(charge.allocation.id).ljust(6)
    project = str(charge.allocation.project).ljust(15)
    resource = str(charge.allocation.resource).ljust(10)
    amount = str(charge.effective_amount)
    return " ".join([id, allocation_id, project, resource, amount])

def refund_format (refund):
    id = str(refund.id).ljust(6)
    charge_id = str(refund.charge.id).ljust(6)
    allocation_id = str(refund.charge.allocation.id).ljust(6)
    project = str(refund.charge.allocation.project).ljust(15)
    resource = str(refund.charge.allocation.resource).ljust(10)
    amount = str(refund.amount)
    return " ".join([id, allocation_id, project, resource, amount])

if __name__ == "__main__":
    console_main()
