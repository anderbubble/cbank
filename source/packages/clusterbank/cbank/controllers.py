import optparse
import os
import sys
import pwd
import ConfigParser
from datetime import datetime
import warnings

import sqlalchemy.exceptions

import clusterbank
from clusterbank import config
from clusterbank.model import \
    Session, user_by_name, project_by_name, resource_by_name, \
    Allocation, Charge, Refund
import clusterbank.cbank.exceptions as exceptions
import clusterbank.cbank.views as views
from clusterbank.cbank.common import get_unit_factor

__all__ = ["main", "report_main", "allocation_main", "charge_main",
    "refund_main"]

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
        except exceptions.CbankError, e:
            print >> sys.stderr, e
            sys.exit(1)
    decorated_func.__name__ = func.__name__
    decorated_func.__doc__ = func.__doc__
    decorated_func.__dict__.update(func.__dict__)
    return decorated_func

def require_admin (func):
    def decorated_func (*args, **kwargs):
        user = get_current_user()
        if not user.is_admin:
            raise exceptions.NotPermitted("must be an admin")
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
        raise exceptions.MissingCommand("specify a command: allocation, charge, refund")
    arg0 = " ".join([sys.argv[0], sys.argv[1]])
    sys.argv = [arg0] + sys.argv[2:]
    if command == "allocation":
        return new_allocation_main()
    elif command == "charge":
        return new_charge_main()
    elif command == "refund":
        return new_refund_main()

def normalize (arg, commands):
    possible_commands = [
        command for command in commands if command.startswith(arg)]
    if not possible_commands or len(possible_commands) > 1:
        raise exceptions.UnknownCommand(arg)
    else:
        return possible_commands[0]

@handle_exceptions
@require_admin
def new_allocation_main ():
    parser = build_allocation_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnknownArguments()
    if not options.project:
        raise exceptions.MissingOption("supply a project")
    if not options.resource:
        raise exceptions.MissingOption("supply a resource")
    if not options.amount:
        raise exceptions.MissingOption("supply an amount")
    if not options.start:
        raise exceptions.MissingOption("supply a start date")
    if not options.expiration:
        raise exceptions.MissingOption("supply an expiration date")
    amount = parse_units(options.amount)
    allocation = Allocation(
        project=options.project, resource=options.resource,
        start=options.start, expiration=options.expiration,
        amount=amount, comment=options.comment)
    Session.save(allocation)
    try:
        Session.commit()
    except ValueError, e:
        raise exceptions.InvalidOptionValue(e)
    views.print_allocation(allocation)

@handle_exceptions
@require_admin
def new_charge_main ():
    parser = build_charge_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnknownArguments()
    if not options.project:
        raise exceptions.MissingOption("supply a project")
    if not options.resource:
        raise exceptions.MissingOption("supply a resource")
    if not options.amount:
        raise exceptions.MissingOption("supply an amount")
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
        raise exceptions.InvalidOptionValue(e)
    views.print_charges(charges)

@handle_exceptions
@require_admin
def new_refund_main ():
    parser = build_refund_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnknownArguments()
    if not options.charge:
        raise exceptions.MissingOption("supply a charge")
    try:
        amount = parse_units(options.amount)
    except TypeError:
        amount = options.amount
    refund = Refund(
        charge=options.charge, amount=amount, comment=options.comment)
    Session.save(refund)
    Session.commit()
    views.print_refund(refund)

@handle_exceptions
def report_main ():
    try:
        command = normalize(sys.argv[1], ["usage", "charges", "projects", "allocations"])
    except (IndexError, exceptions.UnknownCommand):
        command = "usage"
    else:
        arg0 = " ".join([sys.argv[0], sys.argv[1]])
        sys.argv = [arg0] + sys.argv[2:]
    if command == "usage":
        return report_usage_main()
    elif command == "charges":
        return report_charges_main()
    elif command == "projects":
        return report_projects_main()
    elif command == "allocations":
        return report_allocations_main()

@handle_exceptions
def report_usage_main ():
    parser = build_report_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnknownArguments()
    if not options.resources:
        try:
            default_resource = config.get("cbank", "resource")
        except ConfigParser.Error:
            pass
        else:
            resources = [default_resource]
    user = get_current_user()
    if user.is_admin:
        views.print_admin_usage_report(
            projects=options.projects, users=options.users,
            resources=options.resources, after=options.after,
            before=options.before, extra=options.extra)
    else:
        views.print_member_usage_report(user,
            projects=options.projects, users=options.users,
            resources=options.resources, after=options.after,
            before=options.before, extra=options.extra)

@handle_exceptions
def report_projects_main ():
    parser = build_report_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnknownArguments()
    if not options.resources:
        try:
            default_resource = config.get("cbank", "resource")
        except ConfigParser.Error:
            pass
        else:
            resources = [default_resource]
    user = get_current_user()
    if user.is_admin:
        views.print_admin_projects_report(
            projects=options.projects, users=options.users,
            resources=options.resources, after=options.after,
            before=options.before, extra=options.extra)
    else:
        views.print_member_projects_report(user,
            projects=options.projects, users=options.users,
            resources=options.resources, after=options.after,
            before=options.before, extra=options.extra)

@handle_exceptions
def report_allocations_main ():
    parser = build_report_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnknownArguments()
    if not options.resources:
        try:
            default_resource = config.get("cbank", "resource")
        except ConfigParser.Error:
            pass
        else:
            resources = [default_resource]
    user = get_current_user()
    if user.is_admin:
        views.print_admin_allocations_report(
            projects=options.projects, users=options.users,
            resources=options.resources, after=options.after,
            before=options.before, extra=options.extra)
    else:
        views.print_member_allocations_report(user,
            projects=options.projects, users=options.users,
            resources=options.resources, after=options.after,
            before=options.before, extra=options.extra)

@handle_exceptions
def report_charges_main ():
    parser = build_report_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnknownArguments()
    if not options.resources:
        try:
            default_resource = config.get("cbank", "resource")
        except ConfigParser.Error:
            pass
        else:
            resources = [default_resource]
    user = get_current_user()
    if user.is_admin:
        views.print_admin_charges_report(
            projects=options.projects, users=options.users,
            resources=options.resources, after=options.after,
            before=options.before, extra=options.extra)
    else:
        views.print_member_charges_report(user,
            projects=options.projects, users=options.users,
            resources=options.resources, after=options.after,
            before=options.before, extra=options.extra)

def get_current_user ():
    uid = os.getuid()
    try:
        passwd_entry = pwd.getpwuid(uid)
    except KeyError:
        raise exceptions.UnknownUser("Unable to determine the current user.")
    username = passwd_entry[0]
    try:
        user = user_by_name(username)
    except clusterbank.exceptions.NotFound:
        raise exceptions.UnknownUser("User '%s' was not found." % username)
    return user

def parse_units (units):
    units = float(units)
    mul, div = get_unit_factor()
    raw_units = units / mul * div
    raw_units = int(raw_units)
    return raw_units

def build_report_parser ():
    
    usage_str = os.linesep.join([
        "cbank [options] [report]",
        "",
        "reports:",
        "  usage, projects, allocations, charges"])

    version_str = "cbank %s" % clusterbank.__version__
    
    parser = optparse.OptionParser(usage=usage_str, version=version_str)
    parser.add_option(Option("-p", "--project", dest="projects", type="project", action="append",
        help="filter by project NAME", metavar="NAME"))
    parser.add_option(Option("-u", "--user", dest="users", type="user", action="append",
        help="filter by user NAME", metavar="NAME"))
    parser.add_option(Option("-r", "--resource", dest="resources", type="resource", action="append",
        help="filter by resource NAME", metavar="NAME"))
    parser.add_option(Option("-a", "--after", dest="after", type="date",
        help="filter by start DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before", dest="before", type="date",
        help="filter by end DATE", metavar="DATE"))
    parser.add_option(Option("-e", "--extra-data", dest="extra", action="store_true",
        help="display extra data"))
    parser.set_defaults(extra=False, projects=[], users=[], resources=[])
    return parser

def build_allocation_parser ():
    parser = optparse.OptionParser()
    parser.add_option(Option("-p", "--project", type="project", dest="project"))
    parser.add_option(Option("-r", "--resource", type="resource", dest="resource"))
    parser.add_option(Option("-s", "--start", dest="start", type="date"))
    parser.add_option(Option("-e", "--expiration", dest="expiration", type="date"))
    parser.add_option("-a", "--amount", dest="amount", type="float")
    parser.add_option("-m", "--comment", dest="comment")
    return parser

def build_charge_parser ():
    parser = optparse.OptionParser()
    parser.add_option(Option("-p", "--project", type="project", dest="project"))
    parser.add_option(Option("-r", "--resource", type="resource", dest="resource"))
    parser.add_option(Option("-u", "--user", type="user", dest="user"))
    parser.add_option("-a", "--amount", dest="amount", type="float")
    parser.add_option("-m", "--comment", dest="comment")
    return parser

def build_refund_parser ():
    parser = optparse.OptionParser()
    parser.add_option(Option("-c", "--charge", type="charge", dest="charge"))
    parser.add_option("-a", "--amount", dest="amount", type="float")
    parser.add_option("-m", "--comment", dest="comment")
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
            return Session.query(Charge).filter_by(id=value).one()
        except sqlalchemy.exceptions.InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown charge: %i" % (opt, value))
    
    TYPES = optparse.Option.TYPES + ("date", "project", "resource", "user", "charge")
    
    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER['date'] = check_date
    TYPE_CHECKER['project'] = check_project
    TYPE_CHECKER['resource'] = check_resource
    TYPE_CHECKER['user'] = check_user
    TYPE_CHECKER['charge'] = check_charge