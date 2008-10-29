"""Controllers for the cbank interface.

main -- metacontroller that dispatches to report_main and new_main
new_main -- metacontroller that dispatches to creation controllers
report_main -- metacontroller that dispatches to report controllers
new_allocation_main -- creates new allocations
new_charge_main -- creates new charges
new_refund_main -- creates new refunds
report_users_main -- users report
report_projects_main -- projects report
report_allocations_main -- allocations report
report_holds_main -- holds report
report_charges_main -- charges report
"""

import optparse
import os
import sys
import pwd
import time
import ConfigParser
from datetime import datetime, timedelta
from warnings import warn
from textwrap import dedent

from sqlalchemy.orm import eagerload
from sqlalchemy import cast, func, and_
from sqlalchemy.types import Integer
from sqlalchemy.exceptions import InvalidRequestError

import clusterbank
from clusterbank import config
from clusterbank.model import Session, User, Project, Resource, \
    Allocation, Hold, Charge, Refund, \
    user_by_name, project_by_name, resource_by_name, \
    project_members, user_projects, user_projects_owned
from clusterbank.cbank.views import print_allocation, print_charges, \
    print_holds, print_refund, print_users_report, print_projects_report, \
    print_allocations_report, print_holds_report, print_charges_report, \
    print_allocations, print_refunds
from clusterbank.cbank.common import get_unit_factor
from clusterbank.exceptions import NotFound
from clusterbank.cbank.exceptions import CbankException, NotPermitted, \
    UnknownCommand, MissingArgument, UnexpectedArguments, MissingResource, \
    UnknownCharge, UnknownProject, ValueError_, UnknownUser, MissingCommand

__all__ = ["main", "new_main", "report_main",
    "new_allocation_main", "new_charge_main", "new_refund_main"
    "report_users_main", "report_projects_main", "report_allocations_main",
    "report_holds_main", "report_charges_main"]


def dt_strptime (value, format):
    return datetime(*time.strptime(value, format)[0:6])


def handle_exceptions (func):
    def decorated_func (*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            sys.exit(1)
        except CbankException, ex:
            print >> sys.stderr, ex
            sys.exit(ex.exit_code)
    decorated_func.__name__ = func.__name__
    decorated_func.__doc__ = func.__doc__
    decorated_func.__dict__.update(func.__dict__)
    return decorated_func


def require_admin (func):
    def decorated_func (*args, **kwargs):
        user = get_current_user()
        if not user.is_admin:
            raise NotPermitted(user)
        else:
            return func(*args, **kwargs)
    decorated_func.__name__ = func.__name__
    decorated_func.__doc__ = func.__doc__
    decorated_func.__dict__.update(func.__dict__)
    return decorated_func


def help_requested ():
    args = sys.argv[1:]
    return "-h" in args or "--help" in args


@handle_exceptions
def main ():
    try:
        command = normalize(sys.argv[1], ["new", "report", "detail"])
    except (IndexError, UnknownCommand):
        if help_requested():
            print_main_help()
            sys.exit()
        command = "report"
    else:
        replace_command()
    if command == "new":
        return new_main()
    elif command == "report":
        return report_main()
    elif command == "detail":
        return detail_main()


def print_main_help ():
    command = os.path.basename(sys.argv[0])
    message = """\
        usage: %(command)s <subcommand>
        
        A metacommand that dispatches to other subcommands:
          report (default) -- generate reports
          detail -- retrieve details of a specific entity
          new -- create new entities
        
        Arguments (other than -h, --help) will be passed to the
        default subcommand (listed above).
        
        For help with a specific subcommand, run
          %(command)s <command> -h"""
    print dedent(message % {'command':command})


@handle_exceptions
@require_admin
def new_main ():
    commands = ["allocation", "hold", "charge", "refund"]
    try:
        command = normalize(sys.argv[1], commands)
    except UnknownCommand:
        if help_requested():
            print_new_main_help()
            sys.exit()
        else:
            raise
    except IndexError:
        if help_requested():
            print_new_main_help()
            sys.exit()
        else:
            raise MissingCommand(", ".join(commands))
    replace_command()
    if command == "allocation":
        return new_allocation_main()
    elif command == "hold":
        return new_hold_main()
    elif command == "charge":
        return new_charge_main()
    elif command == "refund":
        return new_refund_main()


def print_new_main_help ():
    command = os.path.basename(sys.argv[0])
    message = """\
        usage: %(command)s <entity>
        
        Create clusterbank entities:
          allocation
          charge
          refund
        
        Each entity has its own set of options. For help with a specific
        entity, run
          %(command)s <entity> -h"""
    print dedent(message % {'command':command})


@handle_exceptions
@require_admin
def new_allocation_main ():
    parser = build_new_allocation_parser()
    options, args = parser.parse_args()
    project = pop_project(args, 0)
    amount = pop_amount(args, 0)
    if args:
        raise UnexpectedArguments(args)
    if options.deprecated_comment is not None:
        warn("use of -m is deprecated: use -c instead", DeprecationWarning)
    if not options.resource:
        raise MissingResource()
    comment = options.comment or options.deprecated_comment
    allocation = Allocation(project, options.resource, amount,
        options.start, options.expiration)
    allocation.comment = comment
    if options.commit:
        s = Session()
        s.add(allocation)
        try:
            s.commit()
        except ValueError, ex:
            raise ValueError_(ex)
    print_allocation(allocation)


@handle_exceptions
@require_admin
def new_charge_main ():
    parser = build_new_charge_parser()
    options, args = parser.parse_args()
    project = pop_project(args, 0)
    amount = pop_amount(args, 0)
    if args:
        raise UnexpectedArguments(args)
    if options.deprecated_comment is not None:
        warn("use of -m is deprecated: use -c instead", DeprecationWarning)
    if not options.resource:
        raise MissingResource("resource")
    s = Session()
    allocations = s.query(Allocation).filter_by(
        project=project, resource=options.resource)
    charges = Charge.distributed(allocations, amount)
    for charge in charges:
        charge.user = options.user
        charge.comment = options.comment or options.deprecated_comment
    if options.commit:
        for charge in charges:
            s.add(charge)
        try:
            s.commit()
        except ValueError, ex:
            raise ValueError_(ex)
    print_charges(charges)


@handle_exceptions
@require_admin
def new_hold_main ():
    parser = build_new_hold_parser()
    options, args = parser.parse_args()
    project = pop_project(args, 0)
    amount = pop_amount(args, 0)
    if args:
        raise UnexpectedArguments(args)
    if options.deprecated_comment is not None:
        warn("use of -m is deprecated: use -c instead", DeprecationWarning)
    if not options.resource:
        raise MissingResource("resource")
    s = Session()
    allocations = s.query(Allocation).filter_by(
        project=project, resource=options.resource)
    holds = Hold.distributed(allocations, amount)
    for hold in holds:
        hold.user = options.user
        hold.comment = options.comment or options.deprecated_comment
    if options.commit:
        for hold in holds:
            s.add(hold)
        try:
            s.commit()
        except ValueError, ex:
            raise ValueError_(ex)
    print_holds(holds)


def pop_project (args, index):
    try:
        project_name = args.pop(index)
    except IndexError:
        raise MissingArgument("project")
    try:
        project = project_by_name(project_name)
    except NotFound:
        raise UnknownProject(project_name)
    return project


def pop_charge (args, index):
    try:
        charge_id = args.pop(index)
    except IndexError:
        raise MissingArgument("charge")
    try:
        charge = Session().query(Charge).filter_by(id=charge_id).one()
    except InvalidRequestError:
        raise UnknownCharge(charge_id)
    return charge


def pop_amount (args, index):
    try:
        amount = args.pop(index)
    except IndexError:
        raise MissingArgument("amount")
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
    except MissingArgument:
        amount = charge.effective_amount
    if args:
        raise UnexpectedArguments(args)
    if options.deprecated_comment is not None:
        warn("use of -m is deprecated: use -c instead", DeprecationWarning)
    refund = Refund(charge, amount)
    refund.comment = options.comment or options.deprecated_comment
    if options.commit:
        s = Session()
        s.add(refund)
        try:
            s.commit()
        except ValueError, ex:
            raise ValueError_(ex)
    print_refund(refund)


@handle_exceptions
def report_main ():
    commands = ["users", "projects", "allocations", "holds", "charges"]
    try:
        command = normalize(sys.argv[1], commands)
    except (IndexError, UnknownCommand):
        if help_requested():
            print_report_main_help()
            sys.exit()
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


def print_report_main_help ():
    command = os.path.basename(sys.argv[0])
    message = """\
        usage: %(command)s <report>
        
        Generate clusterbank reports:
          users
          projects (default)
          allocations
          holds
          charges
        
        Arguments (other than -h, --help) will be passed to the
        default report (listed above).
        
        Each report has its own set of options. For help with a specific
        report, run
          %(command)s <report> -h"""
    print dedent(message % {'command':command})


def user_owns_all (user, projects):
    projects_owned = set(user_projects_owned(user))
    return set(projects).issubset(projects_owned)


def project_members_all (projects):
    return set(sum([project_members(project) for project in projects], []))


@handle_exceptions
def report_users_main ():
    parser = build_report_users_parser()
    options, args = parser.parse_args()
    if args:
        raise UnexpectedArguments(args)
    user = get_current_user()
    users = options.users
    projects = options.projects
    if user.is_admin:
        if not users:
            if projects:
                users = project_members_all(projects)
            else:
                users = Session().query(User).all()
    else:
        if not projects:
            projects = user_projects(user)
        print user
        print projects
        print user_owns_all(user, projects)
        if projects and user_owns_all(user, projects):
            if not users:
                users = project_members_all(projects)
        else:
            if not users:
                users = [user]
            elif set(users) != set([user]):
                raise NotPermitted(user)
    resources = options.resources or configured_resources()
    print_users_report(users, projects=projects,
        resources=resources, after=options.after, before=options.before)


def user_projects_all (users):
    return set(sum([user_projects(user) for user in users], []))


@handle_exceptions
def report_projects_main ():
    parser = build_report_projects_parser()
    options, args = parser.parse_args()
    if args:
        raise UnexpectedArguments(args)
    user = get_current_user()
    projects = options.projects
    users = options.users
    if user.is_admin:
        if not projects:
            if users:
                projects = user_projects_all(users)
            else:
                projects = Session().query(Project).all()
    else:
        if not projects:
            projects = user_projects(user)
        allowed_projects = set(user_projects(user) + user_projects_owned(user))
        if not set(projects).issubset(allowed_projects):
            raise NotPermitted(user)
        if not (projects and user_owns_all(user, projects)):
            if not set(users).issubset(set([user])):
                raise NotPermitted(user)
    resources = options.resources or configured_resources()
    print_projects_report(projects, users=users, resources=resources,
        after=options.after, before=options.before)


@handle_exceptions
def report_allocations_main ():
    parser = build_report_allocations_parser()
    options, args = parser.parse_args()
    if options.extra_data is not None:
        warn("use of -e is deprecated: use -c instead", DeprecationWarning)
    if args:
        raise UnexpectedArguments(args)
    user = get_current_user()
    projects = options.projects
    users = options.users
    if user.is_admin:
        if not projects:
            if users:
                projects = user_projects_all(users)
            else:
                projects = Session().query(Project).all()
    else:
        if not projects:
            projects = user_projects(user)
        allowed_projects = set(user_projects(user) + user_projects_owned(user))
        if not set(projects).issubset(allowed_projects):
            raise NotPermitted(user)
        if not (projects and user_owns_all(user, projects)):
            if not set(users).issubset(set([user])):
                raise NotPermitted(user)
    resources = options.resources or configured_resources()
    comments = options.comments or options.extra_data
    allocations = Session().query(Allocation)
    if resources:
        allocations = allocations.filter(Allocation.resource.has(
            Resource.id.in_(resource.id for resource in resources)))
    if projects:
        allocations = allocations.filter(Allocation.project.has(
            Project.id.in_(project.id for project in projects)))
    now = datetime.now()
    if options.after or options.before:
        if options.after:
            allocations = allocations.filter(
                Allocation.expiration > options.after)
        if options.before:
            allocations = allocations.filter(
                Allocation.start <= options.before)
    else:
        allocations = allocations.filter(and_(
            Allocation.expiration > (options.after or now),
            Allocation.start <= (options.before or now)))
    print_allocations_report(allocations.all(), users=users,
        after=options.after, before=options.before, comments=comments)


@handle_exceptions
def report_holds_main ():
    parser = build_report_holds_parser()
    options, args = parser.parse_args()
    if options.extra_data is not None:
        warn("use of -e is deprecated: use -c instead", DeprecationWarning)
    if args:
        raise UnexpectedArguments(args)
    user = get_current_user()
    users = options.users
    projects = options.projects
    if user.is_admin:
        if not users:
            if projects:
                users = project_members_all(projects)
            else:
                users = Session().query(User).all()
    else:
        if not projects:
            projects = user_projects(user)
        if projects and user_owns_all(user, projects):
            pass
        else:
            if not users:
                users = [user]
            elif set(users) != set([user]):
                raise NotPermitted(user)
    resources = options.resources or configured_resources()
    comments = options.comments or options.extra_data
    holds = Session().query(Hold)
    holds = holds.filter(Hold.active==True)
    if users:
        holds = holds.filter(Hold.user.has(User.id.in_(
            user.id for user in users)))
    if projects:
        holds = holds.filter(Hold.allocation.has(Allocation.project.has(
            Project.id.in_(project.id for project in projects))))
    if resources:
        holds = holds.filter(Hold.allocation.has(Allocation.resource.has(
            Resource.id.in_(resource.id for resource in resources))))
    if options.after:
        holds = holds.filter(Hold.datetime >= options.after)
    if options.before:
        holds = holds.filter(Hold.datetime < options.before)
    print_holds_report(holds, comments=comments)


@handle_exceptions
def report_charges_main ():
    parser = build_report_charges_parser()
    parser.set_defaults(after=datetime.now()-timedelta(days=7))
    options, args = parser.parse_args()
    if options.extra_data is not None:
        warn("use of -e is deprecated: use -c instead", DeprecationWarning)
    if args:
        raise UnexpectedArguments(args)
    user = get_current_user()
    member = set(user_projects(user))
    owned = set(user_projects_owned(user))
    like_admin = user.is_admin \
        or (options.projects and set(options.projects).issubset(owned))
    if like_admin:
        users = options.users
        projects = options.projects
    else:
        if options.users and set(options.users) != set([user]):
            raise NotPermitted(user)
        users = [user]
        projects = options.projects or member
    resources = options.resources or configured_resources()
    comments = options.comments or options.extra_data
    
    charges = Session().query(Charge)
    if users:
        charges = charges.filter(Charge.user.has(User.id.in_(
            user.id for user in users)))
    if projects:
        charges = charges.filter(Charge.allocation.has(Allocation.project.has(
            Project.id.in_(project.id for project in projects))))
    if resources:
        charges = charges.filter(Charge.allocation.has(Allocation.resource.has(
            Resource.id.in_(resource.id for resource in resources))))
    if options.after:
        charges = charges.filter(Charge.datetime >= options.after)
    if options.before:
        charges = charges.filter(Charge.datetime < options.before)
    print_charges_report(charges, comments=comments)


@handle_exceptions
def detail_main ():
    if help_requested():
        print_detail_main_help()
        sys.exit()
    commands = ["allocations", "holds", "charges", "refunds"]
    try:
        command = normalize(sys.argv[1], commands)
    except IndexError:
        raise MissingCommand(", ".join(commands))
    replace_command()
    if command == "allocations":
        return detail_allocations_main()
    elif command == "holds":
        return detail_holds_main()
    elif command == "charges":
        return detail_charges_main()
    elif command == "refunds":
        return detail_refunds_main()


def print_detail_main_help ():
    command = os.path.basename(sys.argv[0])
    message = """\
        usage: %(command)s <entity> <id> ...
        
        Retrieve details of clusterbank entities:
          allocations
          holds
          charges
          refunds
        
        Entity ids are available using applicable reports."""
    print dedent(message % {'command':command})


@handle_exceptions
def detail_allocations_main ():
    user = get_current_user()
    s = Session()
    allocations = \
        s.query(Allocation).filter(Allocation.id.in_(sys.argv[1:]))
    if not user.is_admin:
        projects = user_projects(user) + user_projects_owned(user)
        permitted_allocations = []
        for allocation in allocations:
            if not allocation.project in projects:
                print >> sys.stderr, "%s: not permitted: %s" % (
                    allocation.id, user)
            else:
                permitted_allocations.append(allocation)
        allocations = permitted_allocations
    print_allocations(allocations)


@handle_exceptions
def detail_holds_main ():
    user = get_current_user()
    s = Session()
    holds = s.query(Hold).filter(Hold.id.in_(sys.argv[1:]))
    if not user.is_admin:
        owned_projects = user_projects_owned(user)
        permitted_holds = []
        for hold in holds:
            allowed = hold.user is user \
                or hold.allocation.project in owned_projects
            if not allowed:
                print >> sys.stderr, "%s: not permitted: %s" % (hold.id, user)
            else:
                permitted_holds.append(hold)
        holds = permitted_holds
    print_holds(holds)


@handle_exceptions
def detail_charges_main ():
    user = get_current_user()
    s = Session()
    charges = s.query(Charge).filter(Charge.id.in_(sys.argv[1:]))
    if not user.is_admin:
        owned_projects = user_projects_owned(user)
        permitted_charges = []
        for charge in charges:
            allowed = charge.user is user \
                or charge.allocation.project in owned_projects
            if not allowed:
                print >> sys.stderr, "%s: not permitted: %s" % (
                    charge.id, user)
            else:
                permitted_charges.append(charge)
        charges = permitted_charges
    print_charges(charges)


@handle_exceptions
def detail_refunds_main ():
    user = get_current_user()
    s = Session()
    refunds = s.query(Refund).filter(Refund.id.in_(sys.argv[1:]))
    if not user.is_admin:
        owned_projects = user_projects_owned(user)
        permitted_refunds = []
        for refund in refunds:
            allowed = refund.charge.user is user \
                or refund.charge.allocation.project in owned_projects
            if not allowed:
                print >> sys.stderr, "%s: not permitted: %s" % (
                    refund.id, user)
            else:
                permitted_refunds.append(refund)
        refunds = permitted_refunds
    print_refunds(refunds)


def replace_command ():
    arg0 = " ".join([sys.argv[0], sys.argv[1]])
    sys.argv = [arg0] + sys.argv[2:]


def normalize (command, commands):
    possible_commands = [cmd for cmd in commands if cmd.startswith(command)]
    if not possible_commands or len(possible_commands) > 1:
        raise UnknownCommand(command)
    else:
        return possible_commands[0]


def check_users (users, projects=None):
    if users:
        users = users
    elif projects:
        users = set(sum([project_members(project)
            for project in projects], []))
    else:
        current_user = get_current_user()
        users = [current_user]
    return users


def check_projects (projects, users=None):
    if projects:
        projects = projects
    elif users:
        projects = set(sum([user_projects(user) for user in users], []))
    else:
        user = get_current_user()
        member_projects = set(user_projects(user))
        managed_projects = set(user_projects_owned(user))
        projects = member_projects | managed_projects
    return projects


def configured_resources ():
    resource = configured_resource()
    if resource:
        resources = [resource]
    else:
        resources = []
    return resources


def configured_resource ():
    try:
        resource = config.get("cbank", "resource")
    except ConfigParser.Error:
        resource = None
    else:
        resource = resource_by_name(resource)
    return resource


def get_current_user ():
    uid = os.getuid()
    try:
        passwd_entry = pwd.getpwuid(uid)
    except KeyError:
        raise UnknownUser("not in passwd")
    username = passwd_entry[0]
    try:
        user = user_by_name(username)
    except NotFound:
        raise UnknownUser(username)
    return user


def parse_units (units):
    try:
        units = float(units)
    except ValueError:
        raise ValueError_(units)
    mul, div = get_unit_factor()
    raw_units = units / mul * div
    raw_units = int(raw_units)
    return raw_units


def build_report_users_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="report charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="report charges to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="report charges on RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="report charges after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="report charges before (and excluding) DATE", metavar="DATE"))
    parser.set_defaults(projects=[], users=[], resources=[])
    return parser


def build_report_projects_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="report charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="report charges to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="report charges on RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="report charges after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="report charges before (and excluding) DATE", metavar="DATE"))
    parser.set_defaults(projects=[], users=[], resources=[])
    return parser


def build_report_allocations_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="report allocations to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="report allocations for RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after", metavar="DATE",
        dest="after", type="date",
        help="charges after (and including) DATE"))
    parser.add_option(Option("-b", "--before", metavar="DATE",
        dest="before", type="date",
        help="charges before (and excluding) DATE"))
    parser.add_option(Option("-c", "--with-comments",
        dest="comments", action="store_true",
        help="include the comment line for each allocation"))
    parser.add_option(Option("-e", "--extra-data",
        dest="extra_data", action="store_true",
        help="deprecated: use '-c' instead"))
    parser.set_defaults(projects=[], users=[], resources=[], comments=False)
    return parser


def build_report_holds_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="report holds by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="report holds on PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="report holds for RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="report holds after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="report holds before (and excluding) DATE", metavar="DATE"))
    parser.add_option(Option("-c", "--with-comments",
        dest="comments", action="store_true",
        help="include the comment line for each hold"))
    parser.add_option(Option("-e", "--extra-data",
        dest="extra_data", action="store_true",
        help="deprecated: use '-c' instead"))
    parser.set_defaults(projects=[], users=[], resources=[], comments=False)
    return parser


def build_report_charges_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="report charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="report charges to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="report charges for RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="report charges after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="report charges before (and excluding) DATE", metavar="DATE"))
    parser.add_option(Option("-c", "--with-comments",
        dest="comments", action="store_true",
        help="include the comment line for each charge"))
    parser.add_option(Option("-e", "--extra-data",
        dest="extra_data", action="store_true",
        help="deprecated: use '-c' instead"))
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
    parser.add_option("-m", dest="deprecated_comment",
        help="deprecated: use -c instead")
    parser.add_option("-c", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    parser.add_option(Option("-n", dest="commit", action="store_false",
        help="do not save the allocation"))
    now = datetime.now()
    parser.set_defaults(start=now, expiration=datetime(now.year+1, 1, 1),
        commit=True, resource=configured_resource())
    return parser


def build_new_charge_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        type="user", dest="user",
        help="charge made by USER", metavar="USER"))
    parser.add_option(Option("-r", "--resource",
        type="resource", dest="resource",
        help="charge for RESOURCE", metavar="RESOURCE"))
    parser.add_option("-m", dest="deprecated_comment",
        help="deprecated: use -c instead")
    parser.add_option("-c", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    parser.add_option(Option("-n", dest="commit", action="store_false",
        help="do not save the charge"))
    parser.set_defaults(user=get_current_user(),
        commit=True, resource=configured_resource())
    return parser


def build_new_hold_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        type="user", dest="user",
        help="hold for USER", metavar="USER"))
    parser.add_option(Option("-r", "--resource",
        type="resource", dest="resource",
        help="hold for RESOURCE", metavar="RESOURCE"))
    parser.add_option("-m", dest="deprecated_comment",
        help="deprecated: use -c instead")
    parser.add_option("-c", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    parser.add_option(Option("-n", dest="commit", action="store_false",
        help="do not save the hold"))
    parser.set_defaults(user=get_current_user(),
        commit=True, resource=configured_resource())
    return parser


def build_new_refund_parser ():
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-n", dest="commit", action="store_false",
        help="do not save the refund"))
    parser.add_option("-m", dest="deprecated_comment",
        help="deprecated: use -c instead")
    parser.add_option("-c", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    parser.set_defaults(commit=True)
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
                dt = dt_strptime(value, format)
            except ValueError:
                continue
            else:
                # Python can't translate dates before 1900 to a string,
                # causing crashes when trying to build sql with them.
                if dt < datetime(1900, 1, 1):
                    raise optparse.OptionValueError(
                        "option %s: date must be after 1900: %s" % (opt, value))
                else:
                    return dt
        raise optparse.OptionValueError(
            "option %s: invalid date: %s" % (opt, value))
    
    def check_project (self, opt, value):
        try:
            return project_by_name(value)
        except NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown project: %s" % (opt, value))
    
    def check_resource (self, opt, value):
        try:
            return resource_by_name(value)
        except NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown resource: %s" % (opt, value))
    
    def check_user (self, opt, value):
        try:
            return user_by_name(value)
        except NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown user: %s" % (opt, value))
    
    def check_charge (self, opt, value):
        try:
            charge_id = int(value)
        except ValueError:
            raise optparse.OptionValueError(
                "option %s: invalid charge id: %s" % (opt, value))
        try:
            return Session().query(Charge).filter_by(id=charge_id).one()
        except InvalidRequestError:
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

