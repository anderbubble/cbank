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
from textwrap import dedent

from sqlalchemy import and_
from sqlalchemy.exceptions import InvalidRequestError

import clusterbank
from clusterbank import config
from clusterbank.model import User, Project, Resource, Allocation, Hold, \
    Charge, Refund
from clusterbank.controllers import Session, user, project, resource, \
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


def datetime_strptime (value, format):
    """Parse a datetime like datetime.strptime in Python >= 2.5"""
    return datetime(*time.strptime(value, format)[0:6])


def handle_exceptions (func):
    """Decorate a function to intercept exceptions and exit appropriately."""
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
    """Decorate a function to require administrator rights."""
    def decorated_func (*args, **kwargs):
        current_user = get_current_user()
        if not current_user.is_admin:
            raise NotPermitted(current_user)
        else:
            return func(*args, **kwargs)
    decorated_func.__name__ = func.__name__
    decorated_func.__doc__ = func.__doc__
    decorated_func.__dict__.update(func.__dict__)
    return decorated_func


def help_requested ():
    """Detect if help was requested on argv."""
    args = sys.argv[1:]
    return "-h" in args or "--help" in args


@handle_exceptions
def main ():
    """Primary cbank metacommand.
    
    Commands:
    new -- new_main
    report -- report_main (default)
    detail -- detail_main
    """
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
    """Print help for the primary cbank metacommand."""
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
    """Secondary cbank metacommand for creating new entities.
    
    Commands:
    allocation -- new_allocation_main
    hold -- new_hold_main
    charge -- new_charge_main
    refund -- new_refund_main
    """
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
    """Print help for the 'cbank new' metacommand."""
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
    """Create a new allocation."""
    parser = new_allocation_parser()
    options, args = parser.parse_args()
    project_ = pop_project(args, 0)
    amount = pop_amount(args, 0)
    if args:
        raise UnexpectedArguments(args)
    if not options.resource:
        raise MissingResource()
    comment = options.comment or options.deprecated_comment
    allocation = Allocation(project_, options.resource, amount,
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
    """Create a new charge."""
    parser = new_charge_parser()
    options, args = parser.parse_args()
    project_ = pop_project(args, 0)
    amount = pop_amount(args, 0)
    if args:
        raise UnexpectedArguments(args)
    if not options.resource:
        raise MissingResource("resource")
    s = Session()
    allocations = s.query(Allocation).filter_by(
        project=project_, resource=options.resource)
    charges = Charge.distributed(allocations, amount)
    for charge in charges:
        charge.user = options.user
        charge.comment = options.comment
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
    """Create a new hold."""
    parser = new_hold_parser()
    options, args = parser.parse_args()
    project_ = pop_project(args, 0)
    amount = pop_amount(args, 0)
    if args:
        raise UnexpectedArguments(args)
    if not options.resource:
        raise MissingResource("resource")
    s = Session()
    allocations = s.query(Allocation).filter_by(
        project=project_, resource=options.resource)
    holds = Hold.distributed(allocations, amount)
    for hold in holds:
        hold.user = options.user
        hold.comment = options.comment
    if options.commit:
        for hold in holds:
            s.add(hold)
        try:
            s.commit()
        except ValueError, ex:
            raise ValueError_(ex)
    print_holds(holds)


def pop_project (args, index):
    """Pop a project from the front of args."""
    try:
        project_name = args.pop(index)
    except IndexError:
        raise MissingArgument("project")
    try:
        return project(project_name)
    except NotFound:
        raise UnknownProject(project_name)


def pop_charge (args, index):
    """Pop a charge from the front of args."""
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
    """Pop an amount from the front of args."""
    try:
        amount = args.pop(index)
    except IndexError:
        raise MissingArgument("amount")
    amount = parse_units(amount)
    return amount


@handle_exceptions
@require_admin
def new_refund_main ():
    """Create a new refund."""
    parser = new_refund_parser()
    options, args = parser.parse_args()
    charge = pop_charge(args, 0)
    try:
        amount = pop_amount(args, 0)
    except MissingArgument:
        amount = charge.effective_amount()
    if args:
        raise UnexpectedArguments(args)
    refund = Refund(charge, amount)
    refund.comment = options.comment
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
    """Secondary cbank metacommand for reports.
    
    Commands:
    users -- report_users_main
    projects -- report_projects_main
    allocations -- report_allocations_main
    holds -- report_holds_main
    charges -- report_charges_main
    """
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
    """Print help for the report metacommand."""
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


def user_owns_all (user_, projects):
    """Check that the user is an owner of all of a list of projects."""
    projects_owned = set(user_projects_owned(user_))
    return set(projects).issubset(projects_owned)


def project_members_all (projects):
    """Get a list of all the members of all of a list of projects."""
    return set(sum([project_members(project_) for project_ in projects], []))


def user_projects_all (users):
    """Get a list of all projects that have users in a list of users."""
    return set(sum([user_projects(user_) for user_ in users], []))


@handle_exceptions
def report_users_main ():
    """Report charges for users."""
    parser = report_users_parser()
    options, args = parser.parse_args()
    if args:
        raise UnexpectedArguments(args)
    current_user = get_current_user()
    users = options.users
    projects = options.projects
    if current_user.is_admin:
        if not users:
            if projects:
                users = project_members_all(projects)
            else:
                users = Session().query(User).all()
    else:
        if not projects:
            projects = user_projects(current_user)
        if projects and user_owns_all(current_user, projects):
            if not users:
                users = project_members_all(projects)
        else:
            if not users:
                users = [current_user]
            elif set(users) != set([current_user]):
                raise NotPermitted(current_user)
    resources = options.resources or configured_resources()
    print_users_report(users, projects=projects,
        resources=resources, after=options.after, before=options.before)


@handle_exceptions
def report_projects_main ():
    """Report charges and allocations for projects."""
    parser = report_projects_parser()
    options, args = parser.parse_args()
    if args:
        raise UnexpectedArguments(args)
    current_user = get_current_user()
    projects = options.projects
    users = options.users
    if current_user.is_admin:
        if not projects:
            if users:
                projects = user_projects_all(users)
            else:
                projects = Session().query(Project).all()
    else:
        if not projects:
            projects = user_projects(current_user)
        allowed_projects = set(user_projects(current_user) + \
            user_projects_owned(current_user))
        if not set(projects).issubset(allowed_projects):
            raise NotPermitted(current_user)
        if not (projects and user_owns_all(current_user, projects)):
            if not set(users).issubset(set([current_user])):
                raise NotPermitted(current_user)
    resources = options.resources or configured_resources()
    print_projects_report(projects, users=users, resources=resources,
        after=options.after, before=options.before)


@handle_exceptions
def report_allocations_main ():
    """Report charges and allocation for allocations."""
    parser = report_allocations_parser()
    options, args = parser.parse_args()
    if args:
        raise UnexpectedArguments(args)
    current_user = get_current_user()
    projects = options.projects
    users = options.users
    if current_user.is_admin:
        if not projects:
            if users:
                projects = user_projects_all(users)
            else:
                projects = Session().query(Project).all()
    else:
        if not projects:
            projects = user_projects(current_user)
        allowed_projects = set(
            user_projects(current_user) + user_projects_owned(current_user))
        if not set(projects).issubset(allowed_projects):
            raise NotPermitted(current_user)
        if not (projects and user_owns_all(current_user, projects)):
            if not set(users).issubset(set([current_user])):
                raise NotPermitted(current_user)
    resources = options.resources or configured_resources()
    comments = options.comments
    allocations = Session().query(Allocation)
    if resources:
        allocations = allocations.filter(Allocation.resource.has(
            Resource.id.in_(resource.id for resource in resources)))
    if projects:
        allocations = allocations.filter(Allocation.project.has(
            Project.id.in_(project.id for project in projects)))
    if not (options.after or options.before):
        now = datetime.now()
        allocations = allocations.filter(and_(
            Allocation.expiration > now,
            Allocation.start <= now))
    else:
        if options.after:
            allocations = allocations.filter(
                Allocation.expiration > options.after)
        if options.before:
            allocations = allocations.filter(
                Allocation.start <= options.before)
    print_allocations_report(allocations.all(), users=users,
        after=options.after, before=options.before, comments=comments)


@handle_exceptions
def report_holds_main ():
    """Report active holds."""
    parser = report_holds_parser()
    options, args = parser.parse_args()
    if args:
        raise UnexpectedArguments(args)
    current_user = get_current_user()
    users = options.users
    projects = options.projects
    if not current_user.is_admin:
        if not projects:
            projects = user_projects(current_user)
        if projects and user_owns_all(current_user, projects):
            pass
        else:
            if not users:
                users = [current_user]
            elif set(users) != set([current_user]):
                raise NotPermitted(current_user)
    resources = options.resources or configured_resources()
    comments = options.comments
    holds = Session().query(Hold)
    holds = holds.filter(Hold.active==True)
    if users:
        holds = holds.filter(Hold.user.has(User.id.in_(
            user_.id for user_ in users)))
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
    """Report charges."""
    parser = report_charges_parser()
    options, args = parser.parse_args()
    if args:
        raise UnexpectedArguments(args)
    current_user = get_current_user()
    users = options.users
    projects = options.projects
    if not current_user.is_admin:
        if not projects:
            projects = user_projects(current_user)
        if projects and user_owns_all(current_user, projects):
            pass
        else:
            if not users:
                users = [current_user]
            elif set(users) != set([current_user]):
                raise NotPermitted(current_user)
    resources = options.resources or configured_resources()
    comments = options.comments
    charges = Session().query(Charge)
    if users:
        charges = charges.filter(Charge.user.has(User.id.in_(
            user_.id for user_ in users)))
    if projects:
        charges = charges.filter(Charge.allocation.has(Allocation.project.has(
            Project.id.in_(project.id for project in projects))))
    if resources:
        charges = charges.filter(Charge.allocation.has(Allocation.resource.has(
            Resource.id.in_(resource.id for resource in resources))))
    if not (options.after or options.before):
        charges = charges.filter(
            Charge.datetime >= (datetime.now() - timedelta(days=7)))
    else:
        if options.after:
            charges = charges.filter(Charge.datetime >= options.after)
        if options.before:
            charges = charges.filter(Charge.datetime < options.before)
    print_charges_report(charges, comments=comments)


@handle_exceptions
def detail_main ():
    """A metacommand that dispatches to detail functions.
    
    Commands:
    allocations -- detail_allocations_main
    holds -- detail_holds_main
    charges -- detail_charges_main
    refunds -- detail_refunds_main
    """
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
    """Print help for the detail metacommand."""
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
    """Get a detailed view of specific allocations."""
    current_user = get_current_user()
    s = Session()
    allocations = \
        s.query(Allocation).filter(Allocation.id.in_(sys.argv[1:]))
    if not current_user.is_admin:
        projects = user_projects(current_user) + \
            user_projects_owned(current_user)
        permitted_allocations = []
        for allocation in allocations:
            if not allocation.project in projects:
                print >> sys.stderr, "%s: not permitted: %s" % (
                    allocation.id, current_user)
            else:
                permitted_allocations.append(allocation)
        allocations = permitted_allocations
    print_allocations(allocations)


@handle_exceptions
def detail_holds_main ():
    """Get a detailed view of specific holds."""
    current_user = get_current_user()
    s = Session()
    holds = s.query(Hold).filter(Hold.id.in_(sys.argv[1:]))
    if not current_user.is_admin:
        owned_projects = user_projects_owned(current_user)
        permitted_holds = []
        for hold in holds:
            allowed = hold.user is current_user \
                or hold.allocation.project in owned_projects
            if not allowed:
                print >> sys.stderr, "%s: not permitted: %s" % (
                    hold.id, current_user)
            else:
                permitted_holds.append(hold)
        holds = permitted_holds
    print_holds(holds)


@handle_exceptions
def detail_charges_main ():
    """Get a detailed view of specific charges."""
    current_user = get_current_user()
    s = Session()
    charges = s.query(Charge).filter(Charge.id.in_(sys.argv[1:]))
    if not current_user.is_admin:
        owned_projects = user_projects_owned(current_user)
        permitted_charges = []
        for charge in charges:
            allowed = charge.user is current_user \
                or charge.allocation.project in owned_projects
            if not allowed:
                print >> sys.stderr, "%s: not permitted: %s" % (
                    charge.id, current_user)
            else:
                permitted_charges.append(charge)
        charges = permitted_charges
    print_charges(charges)


@handle_exceptions
def detail_refunds_main ():
    """Get a detailed view of specific refunds."""
    current_user = get_current_user()
    s = Session()
    refunds = s.query(Refund).filter(Refund.id.in_(sys.argv[1:]))
    if not current_user.is_admin:
        owned_projects = user_projects_owned(current_user)
        permitted_refunds = []
        for refund in refunds:
            allowed = refund.charge.user is current_user \
                or refund.charge.allocation.project in owned_projects
            if not allowed:
                print >> sys.stderr, "%s: not permitted: %s" % (
                    refund.id, current_user)
            else:
                permitted_refunds.append(refund)
        refunds = permitted_refunds
    print_refunds(refunds)


def replace_command ():
    """Consolidate argv[0] and argv[1] into a single argument."""
    arg0 = " ".join([sys.argv[0], sys.argv[1]])
    sys.argv = [arg0] + sys.argv[2:]


def normalize (command, commands):
    """Determine which of a set of commands is intended.
    
    Arguments:
    command -- the command specified
    commands -- the list of possible commands
    """
    possible_commands = [cmd for cmd in commands if cmd.startswith(command)]
    if not possible_commands or len(possible_commands) > 1:
        raise UnknownCommand(command)
    else:
        return possible_commands[0]


def configured_resource ():
    """Return the configured resource."""
    try:
        name = config.get("cbank", "resource")
    except ConfigParser.Error:
        return None
    else:
        return resource(name)


def configured_resources ():
    """A list of the configures resources."""
    resource_ = configured_resource()
    if resource_:
        resources = [resource_]
    else:
        resources = []
    return resources


def get_current_user ():
    """Return the user for the running user."""
    uid = os.getuid()
    try:
        passwd_entry = pwd.getpwuid(uid)
    except KeyError:
        raise UnknownUser("not in passwd")
    username = passwd_entry[0]
    try:
        return user(username)
    except NotFound:
        raise UnknownUser(username)


def parse_units (units):
    """Convert configured units into the storage unit."""
    try:
        units = float(units)
    except ValueError:
        raise ValueError_(units)
    mul, div = get_unit_factor()
    raw_units = units / mul * div
    raw_units = int(raw_units)
    return raw_units


def report_users_parser ():
    """An optparse parser for the users report."""
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


def report_projects_parser ():
    """An optparse parser for the projects report."""
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


def report_allocations_parser ():
    """An optparse parser for the allocations report."""
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
    parser.add_option(Option("-c", "--comments",
        dest="comments", action="store_true",
        help="include the comment line for each allocation"))
    parser.set_defaults(projects=[], users=[], resources=[], comments=False)
    return parser


def report_holds_parser ():
    """An optparse parser for the holds report."""
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
    parser.add_option(Option("-c", "--comments",
        dest="comments", action="store_true",
        help="include the comment line for each hold"))
    parser.set_defaults(projects=[], users=[], resources=[], comments=False)
    return parser


def report_charges_parser ():
    """An optparse parser for the charges report."""
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
    parser.add_option(Option("-c", "--comments",
        dest="comments", action="store_true",
        help="include the comment line for each charge"))
    parser.set_defaults(projects=[], users=[], resources=[], comments=False)
    return parser


def new_allocation_parser ():
    """An optparse parser for creating new allocations."""
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


def new_charge_parser ():
    """An optparse parser for creating new charges."""
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


def new_hold_parser ():
    """An optparse parser for creating new holds."""
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


def new_refund_parser ():
    """An optparse parser for creating new refunds."""
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
    
    """An extended optparse option with cbank-specific types.
    
    Types:
    date -- parse a datetime from a variety of string formats
    user -- parse a user from its name or id
    project -- parse a project from its name or id
    resource -- parse a resource from its name or id
    """
    
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
        """Parse a datetime from a variety of string formats."""
        for format in self.DATE_FORMATS:
            try:
                dt = datetime_strptime(value, format)
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
        """Parse a project from its name or id."""
        try:
            return project(value)
        except NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown project: %s" % (opt, value))
    
    def check_resource (self, opt, value):
        """Parse a resource from its name or id."""
        try:
            return resource(value)
        except NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown resource: %s" % (opt, value))
    
    def check_user (self, opt, value):
        """Parse a user from its name or id."""
        try:
            return user(value)
        except NotFound:
            raise optparse.OptionValueError(
                "option %s: unknown user: %s" % (opt, value))
    
    TYPES = optparse.Option.TYPES + (
        "date", "project", "resource", "user")
    
    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER['date'] = check_date
    TYPE_CHECKER['project'] = check_project
    TYPE_CHECKER['resource'] = check_resource
    TYPE_CHECKER['user'] = check_user

