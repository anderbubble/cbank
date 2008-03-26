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
console_main -- wrapper for main that suppresses tracebacks

Objects:
parser -- cbank option parser (instance of optparser.OptionParser)
config -- config parser
"""

import sys
import os
import pwd
from datetime import datetime
import time
import optparse
from ConfigParser import SafeConfigParser as ConfigParser, NoSectionError, NoOptionError
from optparse import OptionParser

import sqlalchemy.exceptions

import clusterbank
import clusterbank.model
import clusterbank.exceptions as exceptions
from clusterbank.model import \
    Session, User, Project, Resource, Request, Allocation, Hold, Charge, Refund
import clusterbank.upstream

__all__ = [
    "Option", "parser", "config",
    "console_main", "main",
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
        "hold", "charge", "refund",
    )
    
    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER.update(dict(
        resource = check_resource,
        project = check_project,
        user = check_user,
        date = check_date,
        hold = check_hold,
        charge = check_charge,
        refund = check_refund,
    ))

config = ConfigParser()
config.read(["/etc/clusterbank.conf"])

current_user = pwd.getpwuid(os.getuid())[0]

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
    dest="request", type="int"))
parser.add_option(Option("-a", "--allocation",
    help="specify an allocation by ID", metavar="ID",
    dest="allocation", type="int"))
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
try:
    parser.set_defaults(resource=Resource.by_name(config.get("cbank", "resource")))
except (NoSectionError, NoOptionError):
    pass


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
        print >> stderr
        parser.print_usage(stderr)
        sys.exit(1)

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
    
    assert is_configured(), "clusterbank is not configured"
    
    if options.list:
        
        if directive == "request":
            entities = requests_by_options(options)
            data = request_data
            header = request_header
        elif directive == "allocation":
            entities = allocations_by_options(options)
            data = allocation_data
            header = allocation_header
        elif directive == "hold":
            entities = holds_by_options(options)
            data = hold_data
            header = hold_header
        elif directive == "charge":
            entities = charges_by_options(options)
            data = charge_data
            header = charge_header
        elif directive == "refund":
            entities = refunds_by_options(options)
            data = refund_data
            header = refund_header
        else:
            raise UnknownDirective("list: %s" % directive)
        
        print >> sys.stderr, format(header, header)
        for entity in entities:
            print format(data(entity), header)
    
    else:
        
        if directive == "request":
            require_options(["project", "resource", "amount"], options)
            request = Request(project=options.project, resource=options.resource, amount=options.amount, start=options.start, comment=options.comment)
            Session.commit()
            print format(request_data(request), request_header)
        
        elif directive == "allocation":
            require_admin()
            require_options(["project", "resource", "amount", "expiration"], options)
            if options.request is not None:
                requests = Request.query.filter_by(id=options.request).all()
            else:
                requests = []
            allocation = Allocation(project=options.project, resource=options.resource, requests=requests, start=options.start or datetime.now(), expiration=options.expiration, amount=options.amount, comment=options.comment)
            Session.commit()
            print format(allocation_data(allocation), allocation_header)
        
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
                    allocations = allocations_by_options(options)
                    holds = Hold.distributed(allocations, user=options.user, amount=options.amount, comment=options.comment)
            else:
                allocation = Allocation.query.filter_by(id=options.allocation).one()
                holds = [Hold(allocation=allocation, user=options.user, amount=options.amount, comment=options.comment)]
            Session.commit()
            for hold in holds:
                print format(hold_data(hold), hold_header)
            
        elif directive == "release":
            require_admin()
            holds = holds_by_options(options)
            holds = list(holds) # remember which holds were active
            for hold in holds:
                hold.active = False
            Session.commit()
            for hold in holds:
                print format(hold_data(hold), hold_header)
        
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
                    allocations = allocations_by_options(options)
                    charges = Charge.distributed(allocations, user=options.user, amount=options.amount, comment=options.comment)
            else:
                allocation = Allocation.query.filter_by(id=options.allocation).one()
                charges = [Charge(allocation=allocation, user=options.user, amount=options.amount, comment=options.comment)]
            Session.commit()
            for charge in charges:
                print format(charge_data(charge), charge_header)
        
        elif directive == "refund":
            require_admin()
            require_options(["charge", "amount"], options)
            refund = Refund(charge=options.charge, amount=options.amount, comment=options.comment)
            Session.commit()
            print format(refund_data(refund), refund_header)
        
        else:
            raise UnknownDirective(directive)

def is_configured ():
    return clusterbank.model.metadata.bind is not None \
        and clusterbank.upstream is not None

def parse_directive (directive):
    directives = ("request", "allocation", "hold", "release", "charge", "refund")
    matches = [each for each in directives if each.startswith(directive)]
    if len(matches) == 1:
        return matches[0]
    else:
        return directive

def require_admin (user=None):
    if user is None:
        user = current_user
    try:
        admins = config.get("cbank", "admins")
    except (NoSectionError, NoOptionError):
        admins = []
    else:
        admins = admins.split(",")
    if not user in admins:
        raise NotPermitted()

def require_options (option_list, options):
    for option in option_list:
        if getattr(options, option, None) is None:
            raise MissingOption(option)

def format (data, header):
    return "".join([data[i].ljust(len(header[i])) for i in xrange(len(header))])

def requests_by_options (options):
    requests = Request.query()
    if options.project:
        requests = requests.filter_by(project=options.project)
    if options.resource:
        requests = requests.filter_by(resource=options.resource)
    if options.request is not None:
        requests = requests.filter_by(id=options.request)
    if options.allocation is not None:
        requests = requests.filter(Request.allocations.any(id=options.allocation))
    if options.hold:
        requests = requests.filter(Request.allocations.any(Allocation.holds.contains(options.hold)))
    if options.charge:
        requests = requests.filter(Request.allocations.any(Allocation.charges.contains(options.charge)))
    if options.refund:
        requests = requests.filter(Request.allocations.any(Allocation.charges.any(Charge.refunds.contains(options.refund))))
    return requests

def allocations_by_options (options):
    allocations = Allocation.query()
    if options.project:
        allocations = allocations.filter_by(project=options.project)
    if options.resource:
        allocations = allocations.filter_by(resource=options.resource)
    if options.request is not None:
        allocations = allocations.filter(Allocations.requests.any(id=options.request))
    if options.allocation is not None:
        allocations = allocations.filter_by(id=options.allocation)
    if options.hold:
        allocations = allocations.filter(Allocation.holds.contains(options.hold))
    if options.charge:
        allocations = allocations.filter(Allocation.charges.contains(options.charge))
    if options.refund:
        allocations = allocations.filter(Allocation.charges.any(Charge.refunds.contains(options.refund)))
    return allocations

def holds_by_options (options):
    holds = Hold.query.filter_by(active=True)
    if options.project:
        holds = holds.filter(Hold.allocation.has(project=options.project))
    if options.resource:
        holds = holds.filter(Hold.allocation.has(resource=options.resource))
    if options.request is not None:
        holds = holds.filter(Hold.allocation.has(Allocation.requests.any(id=options.request)))
    if options.allocation is not None:
        holds = holds.filter(Hold.allocation.has(id=options.allocation))
    if options.hold:
        holds = holds.filter_by(id=options.hold.id)
    if options.user:
        holds = holds.filter_by(user=options.user)
    return holds

def charges_by_options (options):
    charges = Charge.query()
    if options.project:
        charges = charges.filter(Charge.allocation.has(project=options.project))
    if options.resource:
        charges = charges.filter(Charge.allocation.has(resource=options.resource))
    if options.request is not None:
        charges = charges.filter(Charge.allocation.has(Allocation.requests.any(id=options.request)))
    if options.allocation is not None:
        charges = charges.filter(Charge.allocation.has(id=options.allocation))
    if options.charge:
        charges = charges.filter_by(id=options.charge.id)
    if options.refund:
        charges = charges.filter(Charge.refunds.contains(options.refund))
    if options.user:
        charges = charges.filter_by(user=options.user)
    return charges

def refunds_by_options (options):
    refunds = Refund.query()
    if options.project:
        refunds = refunds.filter(Refund.charge.has(Charge.allocation.has(project=options.project)))
    if options.resource:
        refunds = refunds.filter(Refund.charge.has(Charge.allocation.has(resource=options.resource)))
    if options.request is not None:
        refunds = refunds.filter(Refund.charge.has(Charge.allocation.has(Allocation.requests.any(id=options.request))))
    if options.allocation is not None:
        refunds = refunds.filter(Refund.charge.has(Charge.allocation.has(id=options.allocation)))
    if options.charge:
        refunds = refunds.filter_by(charge=options.charge)
    if options.refund:
        refunds = refunds.filter_by(id=options.refund.id)
    return refunds

request_header = ["id      ", "project          ", "resource   ", "amount"]
def request_data (request):
    id = str(request.id)
    project = str(request.project)
    resource = str(request.resource)
    amount = str(request.amount)
    return [id, project, resource, amount]

allocation_header = ["id      ", "project          ", "resource   ", "amount"]
def allocation_data (allocation):
    id = str(allocation.id)
    project = str(allocation.project)
    resource = str(allocation.resource)
    amount = "%i/%i" % (allocation.amount - allocation.amount_charged, allocation.amount)
    return [id, project, resource, amount]

hold_header = ["id      ", "project          ", "resource   ", "amount"]
def hold_data (hold):
    id = str(hold.id)
    project = str(hold.allocation.project)
    resource = str(hold.allocation.resource)
    amount = str(hold.amount)
    return [id, project, resource, amount]

charge_header = ["id      ", "date             ", "project          ", "resource   ", "amount"]
def charge_data (charge):
    id = str(charge.id)
    date = charge.allocation.datetime.strftime("%Y-%m-%d %H:%M")
    project = str(charge.allocation.project)
    resource = str(charge.allocation.resource)
    amount = str(charge.effective_amount)
    return [id, date, project, resource, amount]

refund_header = ["id      ", "charge  ", "project          ", "resource   ", "amount"]
def refund_data (refund):
    id = str(refund.id)
    charge = str(refund.charge.id)
    project = str(refund.charge.allocation.project)
    resource = str(refund.charge.allocation.resource)
    amount = str(refund.amount)
    return [id, charge, project, resource, amount]

if __name__ == "__main__":
    console_main()
