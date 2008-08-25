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
from datetime import datetime, timedelta

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
    "report_users_main", "report_projects_main", "report_allocations_main",
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
        command = normalize(sys.argv[1], ["new", "report", "detail"])
    except (IndexError, exceptions.UnknownCommand):
        command = "report"
    else:
        replace_command()
    if command == "new":
        return new_main()
    elif command == "report":
        return report_main()
    elif command == "detail":
        return detail_main()

@handle_exceptions
@require_admin
def new_main ():
    commands = ["allocation", "charge", "refund"]
    try:
        command = normalize(sys.argv[1], commands)
    except IndexError:
        raise exceptions.MissingCommand(", ".join(commands))
    replace_command()
    if command == "allocation":
        return new_allocation_main()
    elif command == "charge":
        return new_charge_main()
    elif command == "refund":
        return new_refund_main()

@handle_exceptions
@require_admin
def new_allocation_main ():
    parser = build_new_allocation_parser()
    options, args = parser.parse_args()
    project = pop_project(args, 0)
    amount = pop_amount(args, 0)
    if args:
        raise exceptions.UnexpectedArguments(args)
    if not options.resource:
        raise exceptions.MissingResource()
    allocation = Allocation(
        project=project, resource=options.resource,
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
    project = pop_project(args, 0)
    amount = pop_amount(args, 0)
    if args:
        raise exceptions.UnexpectedArguments(args)
    if not options.resource:
        raise exceptions.MissingResource("resource")
    allocations = Session.query(Allocation).filter_by(
        project=project, resource=options.resource)
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

def pop_project (args, index):
    try:
        project_name = args.pop(index)
    except IndexError:
        raise exceptions.MissingArgument("project")
    try:
        project = project_by_name(project_name)
    except clusterbank.exceptions.NotFound:
        raise exceptions.UnknownProject(project_name)
    return project

def pop_charge (args, index):
    try:
        charge_id = args.pop(index)
    except IndexError:
        raise exceptions.MissingArgument("charge")
    try:
        charge = Session.query(Charge).filter_by(id=charge_id).one()
    except sqlalchemy.exceptions.InvalidRequestError:
        raise exceptions.UnknownCharge(charge_id)
    return charge

def pop_amount (args, index):
    try:
        amount = args.pop(index)
    except IndexError:
        raise exceptions.MissingArgument("amount")
    amount = parse_units(amount)
    return amount

@handle_exceptions
@require_admin
def new_refund_main ():
    parser = build_new_refund_parser()
    options, args = parser.parse_args()
    charge = pop_charge(args, 0)
    try:
        amount = pop_amount(args, 0)
    except exceptions.MissingArgument:
        amount = charge.effective_amount
    if args:
        raise exceptions.UnexpectedArguments(args)
    refund = Refund(
        charge=charge, amount=amount, comment=options.comment)
    Session.save(refund)
    try:
        Session.commit()
    except ValueError, e:
        raise exceptions.ValueError(e)
    else:
        views.print_refund(refund)

@handle_exceptions
def report_main ():
    commands = ["users", "projects", "allocations", "holds", "charges"]
    try:
        command = normalize(sys.argv[1], commands)
    except (IndexError, exceptions.UnknownCommand):
        command = "projects"
    else:
        replace_command()
    if command == "users":
        return report_users_main()
    elif command == "projects":
        return report_projects_main()
    elif command == "allocations":
        return report_allocations_main()
    elif command == "holds":
        return report_holds_main()
    elif command == "charges":
        return report_charges_main()

@handle_exceptions
def report_users_main ():
    parser = build_report_users_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    users = check_users(options.users, options.projects)
    projects = check_projects(options.projects, users)
    resources = check_resources(options.resources)
    user = get_current_user()
    if not user.is_admin:
        member_projects = set(model.user_projects(user))
        owned_projects = set(model.user_projects_owned(user))
        if not set(users).issubset(set([user])):
            if not set(projects).issubset(owned_projects):
                raise exceptions.NotPermitted(user)
        if not set(projects).issubset(member_projects | owned_projects):
            raise exceptions.NotPermitted(user)
    views.print_users_report(users=users, projects=projects,
        resources=resources, after=options.after, before=options.before)

@handle_exceptions
def report_projects_main ():
    parser = build_report_projects_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    users = options.users
    projects = check_projects(options.projects, options.users)
    resources = check_resources(options.resources)
    user = get_current_user()
    if not user.is_admin:
        member_projects = set(model.user_projects(user))
        owned_projects = set(model.user_projects_owned(user))
        if not set(users).issubset(set([user])):
            if not set(projects).issubset(owned_projects):
                raise exceptions.NotPermitted(user)
        if not set(projects).issubset(member_projects | owned_projects):
            raise exceptions.NotPermitted(user)
    views.print_projects_report(users=users, projects=projects,
        resources=resources, after=options.after, before=options.before)

@handle_exceptions
def report_allocations_main ():
    parser = build_report_allocations_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    users = options.users
    projects = check_projects(options.projects, options.users)
    resources = check_resources(options.resources)
    user = get_current_user()
    if not user.is_admin:
        member_projects = set(model.user_projects(user))
        owned_projects = set(model.user_projects_owned(user))
        if not set(users).issubset(set([user])):
            if not set(projects).issubset(owned_projects):
                raise exceptions.NotPermitted(user)
        if not set(projects).issubset(member_projects | owned_projects):
            raise exceptions.NotPermitted(user)
    views.print_allocations_report(users=users, projects=projects,
        resources=resources, after=options.after, before=options.before,
        comments=options.comments)

@handle_exceptions
def report_holds_main ():
    parser = build_report_holds_parser()
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    users = options.users
    projects = check_projects(options.projects, options.users)
    resources = check_resources(options.resources)
    user = get_current_user()
    if not user.is_admin:
        member_projects = set(model.user_projects(user))
        owned_projects = set(model.user_projects_owned(user))
        if not users:
            if not set(projects).issubset(owned_projects):
                users = [user]
        if not set(users).issubset(set([user])):
            if not set(projects).issubset(owned_projects):
                raise exceptions.NotPermitted(user)
        if not set(projects).issubset(member_projects | owned_projects):
            raise exceptions.NotPermitted(user)
    views.print_holds_report(users=users, projects=projects,
        resources=resources, after=options.after, before=options.before,
        comments=options.comments)

@handle_exceptions
def report_charges_main ():
    parser = build_report_charges_parser()
    parser.set_defaults(after=datetime.now()-timedelta(days=7))
    options, args = parser.parse_args()
    if args:
        raise exceptions.UnexpectedArguments(args)
    users = options.users
    projects = check_projects(options.projects, options.users)
    resources = check_resources(options.resources)
    user = get_current_user()
    if not user.is_admin:
        member_projects = set(model.user_projects(user))
        owned_projects = set(model.user_projects_owned(user))
        if not users:
            if not set(projects).issubset(owned_projects):
                users = [user]
        if not set(users).issubset(set([user])):
            if not set(projects).issubset(owned_projects):
                raise exceptions.NotPermitted(user)
        if not set(projects).issubset(member_projects | owned_projects):
            raise exceptions.NotPermitted(user)
    views.print_charges_report(users=users, projects=projects,
        resources=resources, after=options.after, before=options.before,
        comments=options.comments)

@handle_exceptions
def detail_main ():
    commands = ["allocations", "holds", "charges", "refunds"]
    try:
        command = normalize(sys.argv[1], commands)
    except IndexError:
        raise exceptions.MissingCommand(", ".join(commands))
    replace_command()
    if command == "allocations":
        return detail_allocations_main()
    elif command == "holds":
        return detail_holds_main()
    elif command == "charges":
        return detail_charges_main()
    elif command == "refunds":
        return detail_refunds_main()

@handle_exceptions
def detail_allocations_main ():
    user = get_current_user()
    s = model.Session()
    allocations = \
        s.query(model.Allocation).filter(model.Allocation.id.in_(sys.argv[1:]))
    if not user.is_admin:
        projects = model.user_projects(user) + model.user_projects_owned(user)
        permitted_allocations = []
        for allocation in allocations:
            if not allocation.project in projects:
                print >> sys.stderr, "%s: not permitted: %s" % (
                    allocation.id, user)
            else:
                permitted_allocations.append(allocation)
        allocations = permitted_allocations
    views.print_allocations(allocations)

@handle_exceptions
def detail_holds_main ():
    user = get_current_user()
    s = model.Session()
    holds = s.query(model.Hold).filter(model.Hold.id.in_(sys.argv[1:]))
    if not user.is_admin:
        owned_projects = model.user_projects_owned(user)
        permitted_holds = []
        for hold in holds:
            if not (hold.user is user or hold.allocation.project in owned_projects):
                print >> sys.stderr, "%s: not permitted: %s" % (hold.id, user)
            else:
                permitted_holds.append(hold)
        holds = permitted_holds
    views.print_holds(holds)

@handle_exceptions
def detail_charges_main ():
    user = get_current_user()
    s = model.Session()
    charges = s.query(model.Charge).filter(model.Charge.id.in_(sys.argv[1:]))
    if not user.is_admin:
        owned_projects = model.user_projects_owned(user)
        permitted_charges = []
        for charge in charges:
            if not (charge.user is user or charge.allocation.project in owned_projects):
                print >> sys.stderr, "%s: not permitted: %s" % (charge.id, user)
            else:
                permitted_charges.append(charge)
        charges = permitted_charges
    views.print_charges(charges)

@handle_exceptions
def detail_refunds_main ():
    user = get_current_user()
    s = model.Session()
    refunds = s.query(model.Refund).filter(model.Refund.id.in_(sys.argv[1:]))
    if not user.is_admin:
        owned_projects = model.user_projects_owned(user)
        permitted_refunds = []
        for refund in refunds:
            if not (refund.charge.user is user or refund.charge.allocation.project in owned_projects):
                print >> sys.stderr, "%s: not permitted: %s" % (refund.id, user)
            else:
                permitted_refunds.append(refund)
        refunds = permitted_refunds
    views.print_refunds(refunds)

def replace_command ():
    arg0 = " ".join([sys.argv[0], sys.argv[1]])
    sys.argv = [arg0] + sys.argv[2:]

def normalize (command, commands):
    possible_commands = [cmd for cmd in commands if cmd.startswith(command)]
    if not possible_commands or len(possible_commands) > 1:
        raise exceptions.UnknownCommand(command)
    else:
        return possible_commands[0]

def check_users (users, projects=None):
    if users:
        users = users
    elif projects:
        users = set(sum([model.project_members(project)
            for project in projects], []))
    else:
        current_user = get_current_user()
        users = [current_user]
    return users

def check_projects (projects, users=None):
    if projects:
        projects = projects
    elif users:
        projects = set(sum([model.user_projects(user) for user in users], []))
    else:
        user = get_current_user()
        member_projects = set(model.user_projects(user))
        managed_projects = set(model.user_projects_owned(user))
        projects = member_projects | managed_projects
    return projects

def check_resources (resources):
    """If no resources are specified, check the config file."""
    if resources:
        return resources
    resource = configured_resource()
    if resource:
        return [resource]
    else:
        return []

def configured_resource ():
    try:
        resource = config.get("cbank", "resource")
    except ConfigParser.Error:
        resource = None
    else:
        resource = model.resource_by_name(resource)
    return resource

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
    try:
        units = float(units)
    except ValueError:
        raise exceptions.ValueError(units)
    mul, div = get_unit_factor()
    raw_units = units / mul * div
    raw_units = int(raw_units)
    return raw_units

def build_report_users_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="display charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="display members of and charges to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="display charges on RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="display charges after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="display charges before (and excluding) DATE", metavar="DATE"))
    parser.set_defaults(projects=[], users=[], resources=[])
    return parser

def build_report_projects_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="display charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="display charges to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="display charges on RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="display charges after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="display charges before (and excluding) DATE", metavar="DATE"))
    parser.set_defaults(projects=[], users=[], resources=[])
    return parser

def build_report_allocations_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="display allocations to and charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="display allocations to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="display allocations for RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after", metavar="DATE",
        dest="after", type="date",
        help="display allocations and charges after (and including) DATE"))
    parser.add_option(Option("-b", "--before", metavar="DATE",
        dest="before", type="date",
        help="display allocations and charges before (and excluding) DATE"))
    parser.add_option(Option("-c", "--with-comments",
        dest="comments", action="store_true",
        help="include the comment line for each allocation"))
    parser.set_defaults(projects=[], users=[], resources=[], comments=False)
    return parser

def build_report_holds_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="display holds by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="display holds on PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="display holds for RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="display holds after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="display holds before (and excluding) DATE", metavar="DATE"))
    parser.add_option(Option("-c", "--with-comments",
        dest="comments", action="store_true",
        help="include the comment line for each hold"))
    parser.set_defaults(projects=[], users=[], resources=[], comments=False)
    return parser

def build_report_charges_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="display charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="display charges to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="display charges for RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="display charges after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="display charges before (and excluding) DATE", metavar="DATE"))
    parser.add_option(Option("-c", "--with-comments",
        dest="comments", action="store_true",
        help="include the comment line for each charge"))
    parser.set_defaults(projects=[], users=[], resources=[], comments=False)
    return parser

def build_new_allocation_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-r", "--resource",
        type="resource", dest="resource",
        help="allocate for RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-s", "--start",
        dest="start", type="date",
        help="allocation starts at DATE", metavar="DATE"))
    parser.add_option(Option("-e", "--expiration",
        dest="expiration", type="date",
        help="allocation expires at DATE", metavar="DATE"))
    parser.add_option("-m", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    now = datetime.now()
    parser.set_defaults(start=now, expiration=datetime(now.year+1, 1, 1),
        resource=configured_resource())
    return parser

def build_new_charge_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        type="user", dest="user",
        help="charge made by USER", metavar="USER"))
    parser.add_option(Option("-r", "--resource",
        type="resource", dest="resource",
        help="charge for RESOURCE", metavar="RESOURCE"))
    parser.add_option("-m", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    parser.set_defaults(user=get_current_user(), resource=configured_resource())
    return parser

def build_new_refund_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option("-m", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    return parser


class Option (optparse.Option):
    
    DATE_FORMATS = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%y-%m-%d",
        "%y-%m-%d %H:%M:%S",
        "%y-%m-%d %H:%M",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
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
