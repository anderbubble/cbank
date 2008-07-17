"""Controllers for the cbank interface.

main -- metacontroller that dispatches to report_main and new_main
new_main -- metacontroller that dispatches to creation controllers
report_main -- metacontroller that dispatches to report controllers
new_allocation_main -- creates new allocations
new_charge_main -- creates new charges
new_refund_main -- creates new refunds
report_usage_main -- usage report
report_projects_main -- projects report
report_allocations_main -- allocations report
report_charges_main -- charges report
"""

import optparse
import os
import sys
import pwd
import ConfigParser
from datetime import datetime

import sqlalchemy.exceptions

import clusterbank
from clusterbank import config
import clusterbank.model as model
from clusterbank.model import \
    Session, user_by_name, project_by_name, resource_by_name, \
    Allocation, Charge, Refund
import clusterbank.cbank.exceptions as exceptions
import clusterbank.cbank.views as views
from clusterbank.cbank.common import get_unit_factor

__all__ = ["main", "new_main", "report_main",
    "new_allocation_main", "new_charge_main", "new_refund_main"
    "report_usage_main", "report_projects_main", "report_allocations_main",
    "report_charges_main"]

try:
    dt_strptime = datetime.strprime
except AttributeError:
    import time
    def dt_strptime (value, format):
        return datetime(*time.strptime(value, format)[0:6])

def handle_exceptions (func):
    def decorated_func (*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            sys.exit(1)
        except exceptions.CbankException, e:
            print >> sys.stderr, e
            sys.exit(e.exit_code)
    decorated_func.__name__ = func.__name__
    decorated_func.__doc__ = func.__doc__
    decorated_func.__dict__.update(func.__dict__)
    return decorated_func

def require_admin (func):
    def decorated_func (*args, **kwargs):
        user = get_current_user()
        if not user.is_admin:
            raise exceptions.NotPermitted(user)
        else:
            return func(*args, **kwargs)
    decorated_func.__name__ = func.__name__
    decorated_func.__doc__ = func.__doc__
    decorated_func.__dict__.update(func.__dict__)
    return decorated_func

@handle_exceptions
def main ():
    try:
        command = normalize(sys.argv[1], ["new", "report"])
    except (IndexError, exceptions.UnknownCommand):
        command = "report"
    else:
        arg0 = " ".join([sys.argv[0], sys.argv[1]])
        sys.argv = [arg0] + sys.argv[2:]
    if command == "new":
        return new_main()
    elif command == "report":
        return report_main()

@handle_exceptions
@require_admin
def new_main ():
    try:
        command = normalize(sys.argv[1], ["allocation", "charge", "refund"])
    except IndexError:
        raise exceptions.MissingCommand("specify allocation, charge, or refund")
    arg0 = " ".join([sys.argv[0], sys.argv[1]])
    sys.argv = [arg0] + sys.argv[2:]
    if command == "allocation":
        return new_allocation_main()
    elif command == "charge":
        return new_charge_main()
    elif command == "refund":
        return new_refund_main()

def normalize (command, commands):
    possible_commands = [cmd for cmd in commands if cmd.startswith(command)]
    if not possible_commands or len(possible_commands) > 1:
        raise exceptions.UnknownCommand(command)
    else:
        return possible_commands[0]

@handle_exceptions
@require_admin
def new_allocation_main ():
    parser = build_new_allocation_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    if not options.project:
        raise exceptions.MissingOption("project")
    if not options.resource:
        raise exceptions.MissingOption("resource")
    if not options.amount:
        raise exceptions.MissingOption("amount")
    if not options.start:
        raise exceptions.MissingOption("start")
    if not options.expiration:
        raise exceptions.MissingOption("expiration")
    amount = parse_units(options.amount)
    allocation = Allocation(
        project=options.project, resource=options.resource,
        start=options.start, expiration=options.expiration,
        amount=amount, comment=options.comment)
    Session.save(allocation)
    try:
        Session.commit()
    except ValueError, e:
        raise exceptions.ValueError(e)
    else:
        views.print_allocation(allocation)

@handle_exceptions
@require_admin
def new_charge_main ():
    parser = build_new_charge_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    if not options.project:
        raise exceptions.MissingOption("project")
    if not options.resource:
        raise exceptions.MissingOption("resource")
    if not options.amount:
        raise exceptions.MissingOption("amount")
    allocations = Session.query(Allocation).filter_by(
        project=options.project, resource=options.resource)
    amount = parse_units(options.amount)
    charges = Charge.distributed(allocations,
        user=options.user, amount=amount, comment=options.comment)
    for charge in charges:
        Session.save(charge)
    try:
        Session.commit()
    except ValueError, e:
        raise exceptions.ValueError(e)
    else:
        views.print_charges(charges)

@handle_exceptions
@require_admin
def new_refund_main ():
    parser = build_new_refund_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    if not options.charge:
        raise exceptions.MissingOption("charge")
    try:
        amount = parse_units(options.amount)
    except TypeError:
        amount = options.amount
    refund = Refund(
        charge=options.charge, amount=amount, comment=options.comment)
    Session.save(refund)
    try:
        Session.commit()
    except ValueError, e:
        raise exceptions.ValueError(e)
    else:
        views.print_refund(refund)

@handle_exceptions
def report_main ():
    commands = ["users"]
    try:
        command = normalize(sys.argv[1], commands)
    except (IndexError, exceptions.UnknownCommand):
        command = "users"
    else:
        arg0 = " ".join([sys.argv[0], sys.argv[1]])
        sys.argv = [arg0] + sys.argv[2:]
    if command == "users":
        return report_users_main()

@handle_exceptions
def report_users_main ():
    user = get_current_user()
    parser = build_report_users_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    if user.is_admin:
        if options.projects or options.users:
            users = options.users
        else:
            users = [user]
    else:
        if options.users and set(options.users) != set([user]):
            raise exceptions.NotPermitted(user)
        users = [user]
    resources = check_resources(options.resources)
    views.print_users_report(projects=options.projects, users=users,
        resources=resources, after=options.after, before=options.before)

def check_resources (resources):
    """If no resources are specified, check the config file."""
    if resources:
        return resources
    else:
        try:
            resource = config.get("cbank", "resource")
        except ConfigParser.Error:
            return []
        else:
            resource = model.resource_by_name(resource)
            return [resource]

@handle_exceptions
def report_usage_main ():
    parser = build_report_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    if options.resources:
        resources = options.resources
    else:
        try:
            default_resource = config.get("cbank", "resource")
        except ConfigParser.Error:
            resources = []
        else:
            resources = [default_resource]
    user = get_current_user()
    if user.is_admin:
        views.print_admin_usage_report(
            projects=options.projects, users=options.users,
            resources=resources, after=options.after,
            before=options.before, extra=options.extra)
    else:
        views.print_member_usage_report(user,
            projects=options.projects, users=options.users,
            resources=resources, after=options.after,
            before=options.before, extra=options.extra)

@handle_exceptions
def report_projects_main ():
    parser = build_report_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    if options.resources:
        resources = options.resources
    else:
        try:
            default_resource = config.get("cbank", "resource")
        except ConfigParser.Error:
            resources = []
        else:
            resources = [default_resource]
    user = get_current_user()
    if user.is_admin:
        views.print_admin_projects_report(user,
            projects=options.projects, users=options.users,
            resources=resources, after=options.after,
            before=options.before, extra=options.extra)
    else:
        views.print_member_projects_report(user,
            projects=options.projects, users=options.users,
            resources=resources, after=options.after,
            before=options.before, extra=options.extra)

@handle_exceptions
def report_allocations_main ():
    parser = build_report_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    if options.resources:
        resources = options.resources
    else:
        try:
            default_resource = config.get("cbank", "resource")
        except ConfigParser.Error:
            resources = []
        else:
            resources = [default_resource]
    user = get_current_user()
    if user.is_admin:
        views.print_admin_allocations_report(
            projects=options.projects, users=options.users,
            resources=resources, after=options.after,
            before=options.before, extra=options.extra)
    else:
        views.print_member_allocations_report(user,
            projects=options.projects, users=options.users,
            resources=resources, after=options.after,
            before=options.before, extra=options.extra)

@handle_exceptions
def report_charges_main ():
    parser = build_report_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    if options.resources:
        resources = options.resources
    else:
        try:
            default_resource = config.get("cbank", "resource")
        except ConfigParser.Error:
            resources = []
        else:
            resources = [default_resource]
    user = get_current_user()
    if user.is_admin:
        views.print_admin_charges_report(
            projects=options.projects, users=options.users,
            resources=resources, after=options.after,
            before=options.before, extra=options.extra)
    else:
        views.print_member_charges_report(user,
            projects=options.projects, users=options.users,
            resources=resources, after=options.after,
            before=options.before, extra=options.extra)

def get_current_user ():
    uid = os.getuid()
    try:
        passwd_entry = pwd.getpwuid(uid)
    except KeyError:
        raise exceptions.UnknownUser("not in passwd")
    username = passwd_entry[0]
    try:
        user = user_by_name(username)
    except clusterbank.exceptions.NotFound:
        raise exceptions.UnknownUser(username)
    return user

def parse_units (units):
    units = float(units)
    mul, div = get_unit_factor()
    raw_units = units / mul * div
    raw_units = int(raw_units)
    return raw_units

def build_report_users_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="display members of and charges to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="display charges by USER", metavar="USER"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="display charges to RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="display charges after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="display charges before (and excluding) DATE", metavar="DATE"))
    return parser

def build_report_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="filter by project NAME", metavar="NAME"))
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="filter by user NAME", metavar="NAME"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="filter by resource NAME", metavar="NAME"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="filter by start DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="filter by end DATE", metavar="DATE"))
    parser.add_option(Option("-e", "--extra-data",
        dest="extra", action="store_true",
        help="display extra data"))
    parser.set_defaults(extra=False, projects=[], users=[], resources=[])
    return parser

def build_new_allocation_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-p", "--project",
        type="project", dest="project",
        help="specify project NAME", metavar="NAME"))
    parser.add_option(Option("-r", "--resource",
        type="resource", dest="resource",
        help="specify resource NAME", metavar="NAME"))
    parser.add_option(Option("-s", "--start",
        dest="start", type="date",
        help="specify start DATE", metavar="DATE"))
    parser.add_option(Option("-e", "--expiration",
        dest="expiration", type="date",
        help="specify expiration DATE", metavar="DATE"))
    parser.add_option("-a", "--amount", dest="amount", type="float",
        help="specify allocation AMOUNT", metavar="AMOUNT")
    parser.add_option("-m", "--comment", dest="comment",
        help="add an string COMMENT", metavar="COMMENT")
    return parser

def build_new_charge_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-p", "--project", type="project",
        dest="project", help="specify project NAME", metavar="NAME"))
    parser.add_option(Option("-r", "--resource",
        type="resource", dest="resource",
        help="specify resource NAME", metavar="NAME"))
    parser.add_option(Option("-u", "--user",
        type="user", dest="user",
        help="specify user NAME", metavar="NAME"))
    parser.add_option("-a", "--amount",
        dest="amount", type="float",
        help="specify allocation AMOUNT", metavar="AMOUNT")
    parser.add_option("-m", "--comment",
        dest="comment", help="add an string COMMENT", metavar="COMMENT")
    return parser

def build_new_refund_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-c", "--charge",
        type="charge", dest="charge",
        help="specify charge ID", metavar="ID"))
    parser.add_option("-a", "--amount",
        dest="amount", type="float",
        help="specify allocation AMOUNT", metavar="AMOUNT")
    parser.add_option("-m", "--comment",
        dest="comment", help="add an string COMMENT", metavar="COMMENT")
    return parser


class Option (optparse.Option):
    
    DATE_FORMATS = [
        "%Y-%m-%d",
        "%y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y%m%d",
    ]
    
    def check_date (self, opt, value):
        """Return a datetime from YYYY-MM-DD."""
        for format in self.DATE_FORMATS:
            try:
                return dt_strptime(value, format)
            except ValueError:
                continue
        raise optparse.OptionValueError(
            "option %s: invalid date: %s" % (opt, value))
    
    def check_project (self, opt, value):
        try:
            return project_by_name(value)
        except clusterbank.exceptions.NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown project: %s" % (opt, value))
    
    def check_resource (self, opt, value):
        try:
            return resource_by_name(value)
        except clusterbank.exceptions.NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown resource: %s" % (opt, value))
    
    def check_user (self, opt, value):
        try:
            return user_by_name(value)
        except clusterbank.exceptions.NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown user: %s" % (opt, value))
    
    def check_charge (self, opt, value):
        try:
            charge_id = int(value)
        except ValueError:
            raise optparse.OptionValueError(
                "option %s: invalid charge id: %s" % (opt, value))
        try:
            return Session.query(Charge).filter_by(id=charge_id).one()
        except sqlalchemy.exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown charge: %i" % (opt, value))
    
    TYPES = optparse.Option.TYPES + (
        "date", "project", "resource", "user", "charge")
    
    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER['date'] = check_date
    TYPE_CHECKER['project'] = check_project
    TYPE_CHECKER['resource'] = check_resource
    TYPE_CHECKER['user'] = check_user
    TYPE_CHECKER['charge'] = check_charge
