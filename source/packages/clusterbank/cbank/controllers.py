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
from clusterbank.model import Session, user_by_name, project_by_name, resource_by_name, Allocation, Charge
import clusterbank.cbank.exceptions as exceptions
import clusterbank.cbank.views as views

__all__ = ["main", "report_main", "allocation_main"]

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
    return report_main()

@handle_exceptions
@require_admin
def allocation_main ():
    parser = build_allocation_parser()
    options, args = parser.parse_args()
    if options.project:
        project = project_by_name(options.project)
    else:
        project = None
    if options.resource:
        resource = resource_by_name(options.resource)
    else:
        resource = None
    allocation = Allocation(project=project, resource=resource, start=options.start, expiration=options.expiration, amount=options.amount, comment=options.comment)
    Session.save(allocation)
    try:
        Session.commit()
    except (sqlalchemy.exceptions.IntegrityError, sqlalchemy.exceptions.OperationalError, ValueError):
        raise exceptions.MissingOption("amount, resource, project, start, and expiration are required")
    views.print_allocation(allocation)

@handle_exceptions
@require_admin
def charge_main ():
    parser = build_charge_parser()
    options, args = parser.parse_args()
    project = project_by_name(options.project)
    if not options.resource:
        raise exceptions.MissingOption("resource")
    resource = resource_by_name(options.resource)
    if options.user:
        user = user_by_name(options.user)
    else:
        user = None
    allocations = Session.query(Allocation).filter_by(
        project=project, resource=resource)
    charges = Charge.distributed(allocations,
        user=user, amount=options.amount, comment=options.comment)
    for charge in charges:
        Session.save(charge)
    Session.commit()
    views.print_charges(charges)

@handle_exceptions
def report_main ():
    parser = build_report_parser()
    options, args = parser.parse_args()
    report = get_report(args)
    projects = set(sum(options.projects, []))
    users = set(sum(options.users, []))
    resources = set(sum(options.resources, []))
    if not resources:
        try:
            default_resource = config.get("cbank", "resource")
        except ConfigParser.Error:
            pass
        else:
            resources = [default_resource]
    report(projects=projects, users=users,
        resources=resources, after=options.after,
        before=options.before, extra=options.extra)

def get_report (args):
    try:
        requested_report = args[0]
    except IndexError:
        requested_report = "usage"
    possible_reports = []
    user = get_current_user()
    if user.is_admin:
        if "usage".startswith(requested_report):
            possible_reports.append(views.print_admin_usage_report)
        if "projects".startswith(requested_report):
            possible_reports.append(views.print_admin_projects_report)
        if "allocations".startswith(requested_report):
            possible_reports.append(views.print_admin_allocations_report)
        if "charges".startswith(requested_report):
            possible_reports.append(views.print_admin_charges_report)
    else:
        if "usage".startswith(requested_report):
            possible_reports.append(views.print_member_usage_report)
        if "projects".startswith(requested_report):
            possible_reports.append(views.print_member_projects_report)
        if "allocations".startswith(requested_report):
            possible_reports.append(views.print_member_allocations_report)
        if "charges".startswith(requested_report):
            possible_reports.append(views.print_member_charges_report)
        possible_reports = [
            wrap_member_report(report,user) for report in possible_reports]
    
    if len(possible_reports) != 1:
        raise exceptions.UnknownReport(requested_report)
    else:
        return possible_reports[0]

def wrap_member_report (member_report, user):
    def report_wrapper (**kwargs):
        return member_report(user, **kwargs)
    return report_wrapper

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

def build_report_parser ():
    
    usage_str = os.linesep.join([
        "cbank [options] [report]",
        "",
        "reports:",
        "  usage, projects, allocations, charges"])

    version_str = "cbank %s" % clusterbank.__version__
    
    parser = optparse.OptionParser(usage=usage_str, version=version_str)
    parser.add_option(Option("-p", "--projects", dest="projects", type="csv", action="append",
        help="filter by project NAMES", metavar="NAMES"))
    parser.add_option(Option("-u", "--users", dest="users", type="csv", action="append",
        help="filter by user NAMES", metavar="NAMES"))
    parser.add_option(Option("-r", "--resources", dest="resources", type="csv", action="append",
        help="filter by resource NAMES", metavar="NAMES"))
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
    parser.add_option("-p", "--project", dest="project")
    parser.add_option("-r", "--resource", dest="resource")
    parser.add_option(Option("-s", "--start", dest="start", type="date"))
    parser.add_option(Option("-e", "--expiration", dest="expiration", type="date"))
    parser.add_option("-a", "--amount", dest="amount", type="int")
    parser.add_option("-c", "--comment", dest="comment")
    return parser

def build_charge_parser ():
    parser = optparse.OptionParser()
    parser.add_option("-p", "--project", dest="project")
    parser.add_option("-r", "--resource", dest="resource")
    parser.add_option("-u", "--user", dest="user")
    parser.add_option("-a", "--amount", dest="amount", type="int")
    parser.add_option("-c", "--comment", dest="comment", type="string")
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
            "option %s: invalid date: %r" % (opt, value))
    
    def check_csv (self, opt, value):
        if value:
            if "," in value:
                warnings.warn("CSV options are deprecated.", DeprecationWarning)
            return value.split(",")
        else:
            return []
    
    TYPES = optparse.Option.TYPES + ("date", "csv")
    
    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER['date'] = check_date
    TYPE_CHECKER['csv'] = check_csv
