"""Controllers for the cbank interface.

main -- metacontroller that dispatches to list_main and new_main
new_main -- metacontroller that dispatches to creation controllers
list_main -- metacontroller that dispatches to list controllers
import_main -- metacontroller that dispatches to import controllers
edit_main -- metacontroller that dispatches to edit controllers
new_allocation_main -- creates new allocations
new_charge_main -- creates new charges
new_refund_main -- creates new refunds
import_jobs_main -- imports pbs jobs
list_users_main -- users list
list_projects_main -- projects list
list_allocations_main -- allocations list
list_holds_main -- holds list
list_jobs_main -- jobs list
list_charges_main -- charges list
edit_allocation_main -- edit allocations
edit_hold_main -- edit holds
edit_charge_main -- edit charges
edit_refund_main -- edit refunds
"""


import optparse
import os
import sys
import pwd
import time
import ConfigParser
from datetime import datetime, timedelta
from textwrap import dedent

from sqlalchemy import and_, or_
from sqlalchemy.exceptions import InvalidRequestError, IntegrityError
from sqlalchemy.orm import eagerload

import clusterbank
from clusterbank import config
from clusterbank.model import User, Project, Resource, Allocation, Hold, \
    Job, Charge, Refund
from clusterbank.controllers import (Session, user, project, resource,
    job_from_pbs)
from clusterbank.cbank.views import (print_allocation, print_charge,
    print_charges, print_hold, print_holds, print_refund, print_users_list,
    print_projects_list, print_allocations_list, print_holds_list,
    print_jobs_list, print_charges_list, print_allocations, print_refunds,
    print_jobs)
from clusterbank.cbank.common import get_unit_factor
from clusterbank.exceptions import NotFound
from clusterbank.cbank.exceptions import (CbankException, NotPermitted,
    UnknownCommand, MissingArgument, UnexpectedArguments, MissingResource,
    UnknownAllocation, UnknownCharge, UnknownProject, ValueError_,
    UnknownUser, MissingCommand, HasChildren)


__all__ = ["main", "new_main", "import_main", "list_main",
    "new_allocation_main", "new_charge_main", "new_refund_main",
    "import_jobs_main", "list_users_main", "list_projects_main",
    "list_allocations_main", "list_holds_main", "list_charges_main"]


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
    list -- list_main (default)
    detail -- detail_main
    edit -- edit_main
    import -- import-main
    """
    try:
        command = normalize(sys.argv[1],
            ["new", "import", "list", "detail", "edit"])
    except (IndexError, UnknownCommand):
        if help_requested():
            print_main_help()
            sys.exit()
        command = "list"
    else:
        replace_command()
    if command == "new":
        return new_main()
    elif command == "import":
        return import_main()
    elif command == "list":
        return list_main()
    elif command == "detail":
        return detail_main()
    elif command == "edit":
        return edit_main()


def print_main_help ():
    """Print help for the primary cbank metacommand."""
    command = os.path.basename(sys.argv[0])
    message = """\
        usage: %(command)s <subcommand>
        
        A metacommand that dispatches to other subcommands:
          list (default) -- generate lists
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
    comment = options.comment
    allocation = Allocation(project_, options.resource, amount,
        options.start, options.end)
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
    try:
        charges = Charge.distributed(allocations, amount)
    except ValueError, ex:
        raise ValueError_(ex)
    for charge in charges:
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


@handle_exceptions
@require_admin
def import_main ():
    """Secondary cbank metacommand for importing entities.
    
    Commands:
    jobs -- import_jobs_main
    """
    commands = ["jobs"]
    try:
        command = normalize(sys.argv[1], commands)
    except UnknownCommand:
        if help_requested():
            print_import_main_help()
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
    if command == "jobs":
        return import_jobs_main()


def print_import_main_help ():
    """Print help for the 'cbank import' metacommand."""
    command = os.path.basename(sys.argv[0])
    message = """\
        usage: %(command)s <entity>
        
        Import clusterbank entities:
          jobs
        
        Each entity has its own set of options. For help with a specific
        entity, run
          %(command)s <entity> -h"""
    print dedent(message % {'command':command})


@handle_exceptions
@require_admin
def import_jobs_main ():
    """Import jobs from pbs accounting logs."""
    parser = import_jobs_parser()
    options, args = parser.parse_args()
    if args:
        raise UnexpectedArguments(args)
    s = Session()
    counter = 0
    for line in read(sys.stdin):
        try:
            job = job_from_pbs(line)
        except ValueError, e:
            print >> sys.stderr, e
            continue
        else:
            if options.verbose:
                print >> sys.stderr, job
            counter += 1
            if counter >= 100:
                s.commit()
                counter = 0
    if counter != 0:
        s.commit()


def read (f):
    for line in f:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        yield line


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


def pop_allocation (args, index):
    """Pop an allocation from the front of args."""
    try:
        allocation_id = args.pop(index)
    except IndexError:
        raise MissingArgument("allocation")
    try:
        allocation = Session.query(Allocation).filter_by(
            id=allocation_id).one()
    except InvalidRequestError:
        raise UnknownAllocation(allocation_id)
    return allocation


def pop_hold (args, index):
    """Pop a hold from the front of args."""
    try:
        hold_id = args.pop(index)
    except IndexError:
        raise MissingArgument("hold")
    try:
        hold = Session.query(Hold).filter_by(
            id=hold_id).one()
    except InvalidRequestError:
        raise UnknownAllocation(hold_id)
    return hold


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


def pop_refund (args, index):
    """Pop a refund from the front of args."""
    try:
        refund_id = args.pop(index)
    except IndexError:
        raise MissingArgument("refund")
    try:
        refund = Session().query(Refund).filter_by(id=refund_id).one()
    except InvalidRequestError:
        raise UnknownCharge(refund_id)
    return refund


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
def list_main ():
    """Secondary cbank metacommand for lists.
    
    Commands:
    users -- list_users_main
    projects -- list_projects_main
    allocations -- list_allocations_main
    holds -- list_holds_main
    charges -- list_charges_main
    """
    commands = ["users", "projects", "allocations", "holds", "jobs", "charges"]
    try:
        command = normalize(sys.argv[1], commands)
    except (IndexError, UnknownCommand):
        if help_requested():
            print_list_main_help()
            sys.exit()
        command = "projects"
    else:
        replace_command()
    if command == "users":
        return list_users_main()
    elif command == "projects":
        return list_projects_main()
    elif command == "allocations":
        return list_allocations_main()
    elif command == "holds":
        return list_holds_main()
    elif command == "jobs":
        return list_jobs_main()
    elif command == "charges":
        return list_charges_main()


def print_list_main_help ():
    """Print help for the list metacommand."""
    command = os.path.basename(sys.argv[0])
    message = """\
        usage: %(command)s <list>
        
        Generate clusterbank lists:
          users
          projects (default)
          allocations
          holds
          charges
        
        Arguments (other than -h, --help) will be passed to the
        default list (listed above).
        
        Each list has its own set of options. For help with a specific
        list, run
          %(command)s <list> -h"""
    print dedent(message % {'command':command})


def user_admins_all (user_, projects):
    """Check that the user is an admin of all of a list of projects."""
    admin_projects = set(user_.admin_projects)
    return set(projects).issubset(admin_projects)


def project_members_all (projects):
    """Get a list of all the members of all of a list of projects."""
    return set(sum([project_.members for project_ in projects], []))


def user_projects_all (users):
    """Get a list of all projects that have users in a list of users."""
    return set(sum([user_.projects for user_ in users], []))


@handle_exceptions
def list_users_main ():
    """List charges for users."""
    parser = list_users_parser()
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
            projects = current_user.projects
        if projects and user_admins_all(current_user, projects):
            if not users:
                users = project_members_all(projects)
        else:
            if not users:
                users = [current_user]
            elif set(users) != set([current_user]):
                raise NotPermitted(current_user)
    resources = options.resources or configured_resources()
    print_users_list(users, projects=projects,
        resources=resources, after=options.after, before=options.before,
        truncate=(not options.long))


@handle_exceptions
def list_projects_main ():
    """List charges and allocations for projects."""
    parser = list_projects_parser()
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
            projects = current_user.projects
        allowed_projects = set(current_user.projects
            + current_user.admin_projects)
        if not set(projects).issubset(allowed_projects):
            raise NotPermitted(current_user)
        if not (projects and user_admins_all(current_user, projects)):
            if not set(users).issubset(set([current_user])):
                raise NotPermitted(current_user)
    resources = options.resources or configured_resources()
    print_projects_list(projects, users=users, resources=resources,
        after=options.after, before=options.before,
        truncate=(not options.long))


@handle_exceptions
def list_allocations_main ():
    """List charges and allocation for allocations."""
    parser = list_allocations_parser()
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
            projects = current_user.projects
        allowed_projects = set(
            current_user.projects + current_user.admin_projects)
        if not set(projects).issubset(allowed_projects):
            raise NotPermitted(current_user)
        if not (projects and user_admins_all(current_user, projects)):
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
            Allocation.end > now,
            Allocation.start <= now))
    else:
        if options.after:
            allocations = allocations.filter(
                Allocation.end > options.after)
        if options.before:
            allocations = allocations.filter(
                Allocation.start <= options.before)
    print_allocations_list(allocations.all(), users=users,
        after=options.after, before=options.before, comments=comments,
        truncate=(not options.long))


@handle_exceptions
def list_holds_main ():
    """List active holds."""
    parser = list_holds_parser()
    options, args = parser.parse_args()
    if args:
        raise UnexpectedArguments(args)
    current_user = get_current_user()
    users = options.users
    projects = options.projects
    if not current_user.is_admin:
        if not projects:
            projects = current_user.projects
        if projects and user_admins_all(current_user, projects):
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
        holds = holds.filter(Hold.job.has(Job.user.has(User.id.in_(
            user_.id for user_ in users))))
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
    if options.jobs:
        holds = holds.filter(Hold.job.has(Job.id.in_(
            job.id for job in options.jobs)))
    print_holds_list(holds, comments=comments, truncate=(not options.long))


@handle_exceptions
def list_jobs_main ():
    """List jobs."""
    options, args = list_jobs_parser().parse_args()
    if args:
        raise UnexpectedArguments(args)
    current_user = get_current_user()
    users = options.users
    projects = options.projects
    if not current_user.is_admin:
        if not projects:
            projects = current_user.projects
        if projects and user_admins_all(current_user, projects):
            pass
        else:
            if not users:
                users = [current_user]
            elif set(users) != set([current_user]):
                raise NotPermitted(current_user)
    resources = options.resources or configured_resources()
    jobs = Session().query(Job).order_by(Job.ctime).options(
        eagerload(Job.charges, Charge.refunds))
    if users:
        jobs = jobs.filter(Job.user.has(User.id.in_(
            user_.id for user_ in users)))
    if projects:
        jobs = jobs.filter(Job.account.has(Project.id.in_(
            project_.id for project_ in projects)))
    if options.after:
        jobs = jobs.filter(or_(Job.start >= options.after,
            Job.end > options.after))
    if options.before:
        jobs = jobs.filter(or_(Job.start < options.before,
            Job.end <= options.before))
    if resources:
        jobs = jobs.filter(Job.charges.any(Charge.allocation.has(
            Allocation.resource.has(Resource.id.in_(
            resource.id for resource in resources)))))
    print_jobs_list(jobs, truncate=(not options.long))


@handle_exceptions
def list_charges_main ():
    """List charges."""
    parser = list_charges_parser()
    options, args = parser.parse_args()
    if args:
        raise UnexpectedArguments(args)
    current_user = get_current_user()
    users = options.users
    projects = options.projects
    if not current_user.is_admin:
        if not projects:
            projects = current_user.projects
        if projects and user_admins_all(current_user, projects):
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
        charges = charges.filter(Charge.job.has(Job.user.has(User.id.in_(
            user_.id for user_ in users))))
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
    if options.jobs:
        charges = charges.filter(Charge.job.has(Job.id.in_(
            job.id for job in options.jobs)))
    print_charges_list(charges, comments=comments,
        truncate=(not options.long))


@handle_exceptions
def detail_main ():
    """A metacommand that dispatches to detail functions.
    
    Commands:
    allocations -- detail_allocations_main
    holds -- detail_holds_main
    jobs -- detail_jobs_main
    charges -- detail_charges_main
    refunds -- detail_refunds_main
    """
    if help_requested():
        print_detail_main_help()
        sys.exit()
    commands = ["allocations", "holds", "jobs", "charges", "refunds"]
    try:
        command = normalize(sys.argv[1], commands)
    except IndexError:
        raise MissingCommand(", ".join(commands))
    replace_command()
    if command == "allocations":
        return detail_allocations_main()
    elif command == "holds":
        return detail_holds_main()
    elif command == "jobs":
        return detail_jobs_main()
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
          jobs
          charges
          refunds
        
        Entity ids are available using applicable lists."""
    print dedent(message % {'command':command})


@handle_exceptions
def detail_allocations_main ():
    """Get a detailed view of specific allocations."""
    current_user = get_current_user()
    s = Session()
    allocations = \
        s.query(Allocation).filter(Allocation.id.in_(sys.argv[1:]))
    if not current_user.is_admin:
        projects = current_user.projects + current_user.admin_projects
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
        admin_projects = current_user.admin_projects
        permitted_holds = []
        for hold in holds:
            allowed = hold.user is current_user \
                or hold.allocation.project in admin_projects
            if not allowed:
                print >> sys.stderr, "%s: not permitted: %s" % (
                    hold.id, current_user)
            else:
                permitted_holds.append(hold)
        holds = permitted_holds
    print_holds(holds)


@handle_exceptions
def detail_jobs_main ():
    """Get a detailed view of specific jobs."""
    current_user = get_current_user()
    s = Session()
    jobs = s.query(Job).filter(Job.id.in_(sys.argv[1:]))
    if not current_user.is_admin:
        admin_projects = current_user.admin_projects
        permitted_jobs = []
        for job in jobs:
            allowed = (job.user is current_user
                or job.account in admin_projects)
            if not allowed:
                print >> sys.stderr, "%s: not permitted: %s" % (
                    job.id, current_user)
            else:
                permitted_jobs.append(job)
        jobs = permitted_jobs
    print_jobs(jobs)


@handle_exceptions
def detail_charges_main ():
    """Get a detailed view of specific charges."""
    current_user = get_current_user()
    s = Session()
    charges = s.query(Charge).filter(Charge.id.in_(sys.argv[1:]))
    if not current_user.is_admin:
        admin_projects = current_user.admin_projects
        permitted_charges = []
        for charge in charges:
            allowed = ((
                charge.job and charge.job.user is current_user)
                or charge.allocation.project in admin_projects)
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
        admin_projects = current_user.admin_projects
        permitted_refunds = []
        for refund in refunds:
            
            allowed = (
                (refund.charge.job and refund.charge.job.user and
                    refund.charge.job.user is current_user)
                or refund.charge.allocation.project in admin_projects)
            if not allowed:
                print >> sys.stderr, "%s: not permitted: %s" % (
                    refund.id, current_user)
            else:
                permitted_refunds.append(refund)
        refunds = permitted_refunds
    print_refunds(refunds)


@handle_exceptions
@require_admin
def edit_main ():
    """Secondary cbank metacommand for editing existing entities.
    
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
            print_edit_main_help()
            sys.exit()
        else:
            raise
    except IndexError:
        if help_requested():
            print_edit_main_help()
            sys.exit()
        else:
            raise MissingCommand(", ".join(commands))
    replace_command()
    if command == "allocation":
        return edit_allocation_main()
    elif command == "hold":
        return edit_hold_main()
    elif command == "charge":
        return edit_charge_main()
    elif command == "refund":
        return edit_refund_main()


def print_edit_main_help ():
    """Print help for the 'cbank edit' metacommand."""
    command = os.path.basename(sys.argv[0])
    message = """\
        usage: %(command)s <entity>
        
        Edit clusterbank entities:
          allocation
          charge
          refund
        
        Each entity has its own set of options. For help with a specific
        entity, run
          %(command)s <entity> -h"""
    print dedent(message % {'command':command})


@handle_exceptions
@require_admin
def edit_allocation_main ():
    """Edit an existing allocation."""
    parser = edit_allocation_parser()
    options, args = parser.parse_args()
    allocation = pop_allocation(args, 0)
    if args:
        raise UnexpectedArguments(args)
    if options.delete:
        Session.delete(allocation)
        if options.commit:
            try:
                Session.commit()
            except IntegrityError:
                Session.rollback()
                raise HasChildren("%s has child entities" % allocation)
    else:
        if options.start is not None:
            allocation.start = options.start
        if options.end is not None:
            allocation.end = options.end
        if options.comment is not None:
            allocation.comment = options.comment
        if options.commit:
            Session.commit()
    print_allocation(allocation)


@handle_exceptions
@require_admin
def edit_hold_main ():
    """Edit an existing hold."""
    parser = edit_hold_parser()
    options, args = parser.parse_args()
    hold = pop_hold(args, 0)
    if args:
        raise UnexpectedArguments(args)
    if options.comment is not None:
        hold.comment = options.comment
    if options.active is not None:
        hold.active = options.active
    if options.commit:
        Session.commit()
    print_hold(hold)


@handle_exceptions
@require_admin
def edit_charge_main ():
    """Edit an existing charge."""
    parser = edit_charge_parser()
    options, args = parser.parse_args()
    charge = pop_charge(args, 0)
    if args:
        raise UnexpectedArguments(args)
    if options.delete:
        Session.delete(charge)
        if options.commit:
            try:
                Session.commit()
            except IntegrityError:
                Session.rollback()
                raise HasChildren("%s has child entities" % charge)
    else:
        if options.allocation:
            charge.allocation = options.allocation
        if options.comment is not None:
            charge.comment = options.comment
        if options.commit:
            Session.commit()
    print_charge(charge)


@handle_exceptions
@require_admin
def edit_refund_main ():
    """Edit an existing refund."""
    parser = edit_refund_parser()
    options, args = parser.parse_args()
    refund = pop_refund(args, 0)
    if args:
        raise UnexpectedArguments(args)
    if options.delete:
        Session.delete(refund)
    else:
        if options.comment is not None:
            refund.comment = options.comment
    if options.commit:
        Session.commit()
    print_refund(refund)


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


def list_users_parser ():
    """An optparse parser for the users list."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="list charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="list charges to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="list charges on RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="list charges after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="list charges before (and excluding) DATE", metavar="DATE"))
    parser.add_option(Option("-l", "--long",
        dest="long", action="store_true",
        help="do not truncate long strings"))
    parser.set_defaults(projects=[], users=[], resources=[], long=False)
    return parser


def list_projects_parser ():
    """An optparse parser for the projects list."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="list charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="list charges to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="list charges on RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="list charges after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="list charges before (and excluding) DATE", metavar="DATE"))
    parser.add_option(Option("-l", "--long",
        dest="long", action="store_true",
        help="do not truncate long strings"))
    parser.set_defaults(projects=[], users=[], resources=[], long=False)
    return parser


def list_allocations_parser ():
    """An optparse parser for the allocations list."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="list allocations to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="list allocations for RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after", metavar="DATE",
        dest="after", type="date",
        help="charges after (and including) DATE"))
    parser.add_option(Option("-b", "--before", metavar="DATE",
        dest="before", type="date",
        help="charges before (and excluding) DATE"))
    parser.add_option(Option("-c", "--comments",
        dest="comments", action="store_true",
        help="include the comment line for each allocation"))
    parser.add_option(Option("-l", "--long",
        dest="long", action="store_true",
        help="do not truncate long strings"))
    parser.set_defaults(projects=[], users=[], resources=[], comments=False,
        long=False)
    return parser


def list_holds_parser ():
    """An optparse parser for the holds list."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="list holds by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="list holds on PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="list holds for RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="list holds after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="list holds before (and excluding) DATE", metavar="DATE"))
    parser.add_option(Option("-c", "--comments",
        dest="comments", action="store_true",
        help="include the comment line for each hold"))
    parser.add_option(Option("-l", "--long",
        dest="long", action="store_true",
        help="do not truncate long strings"))
    parser.add_option(Option("-j", "--job",
        dest="jobs", type="job", action="append",
        help="list charges related to JOB", metavar="JOB"))
    parser.set_defaults(projects=[], users=[], resources=[], jobs=[],
        comments=False, long=False)
    return parser


def list_jobs_parser ():
    """An optparse parser for the jobs list."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="list jobs by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="list jobs by PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="list charges for RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="list jobs after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="list jobs before (and excluding) DATE", metavar="DATE"))
    parser.add_option(Option("-l", "--long",
        dest="long", action="store_true",
        help="do not truncate long strings"))
    parser.set_defaults(long=False)
    return parser


def list_charges_parser ():
    """An optparse parser for the charges list."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-u", "--user",
        dest="users", type="user", action="append",
        help="list charges by USER", metavar="USER"))
    parser.add_option(Option("-p", "--project",
        dest="projects", type="project", action="append",
        help="list charges to PROJECT", metavar="PROJECT"))
    parser.add_option(Option("-r", "--resource",
        dest="resources", type="resource", action="append",
        help="list charges for RESOURCE", metavar="RESOURCE"))
    parser.add_option(Option("-a", "--after",
        dest="after", type="date",
        help="list charges after (and including) DATE", metavar="DATE"))
    parser.add_option(Option("-b", "--before",
        dest="before", type="date",
        help="list charges before (and excluding) DATE", metavar="DATE"))
    parser.add_option(Option("-c", "--comments",
        dest="comments", action="store_true",
        help="include the comment line for each charge"))
    parser.add_option(Option("-l", "--long",
        dest="long", action="store_true",
        help="do not truncate long strings"))
    parser.add_option(Option("-j", "--job",
        dest="jobs", type="job", action="append",
        help="list charges related to JOB", metavar="JOB"))
    parser.set_defaults(projects=[], users=[], resources=[], jobs=[],
        comments=False, long=False)
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
    parser.add_option(Option("-e", "--end",
        dest="end", type="date",
        help="allocation expires at DATE", metavar="DATE"))
    parser.add_option("-c", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    parser.add_option(Option("-n", dest="commit", action="store_false",
        help="do not save the allocation"))
    now = datetime.now()
    parser.set_defaults(start=now, end=datetime(now.year+1, 1, 1),
        commit=True, resource=configured_resource())
    return parser


def new_charge_parser ():
    """An optparse parser for creating new charges."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-r", "--resource",
        type="resource", dest="resource",
        help="charge for RESOURCE", metavar="RESOURCE"))
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
    parser.add_option("-c", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    parser.set_defaults(commit=True)
    return parser


def edit_allocation_parser ():
    """An optparse parser for editing existing allocations."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-D", "--delete",
        dest="delete", action="store_true",
        help="delete the allcation"))
    parser.add_option(Option("-s", "--start",
        dest="start", type="date",
        help="allocation starts at DATE", metavar="DATE"))
    parser.add_option(Option("-e", "--end",
        dest="end", type="date",
        help="allocation expires at DATE", metavar="DATE"))
    parser.add_option("-c", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    parser.add_option(Option("-n", dest="commit", action="store_false",
        help="do not save changes to the allocation"))
    parser.set_defaults(commit=True, delete=False)
    return parser


def edit_hold_parser ():
    """An optparse parser for editing existing holds."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option("-c", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    parser.add_option("-d", "--deactivate", action="store_false",
        dest="active", help="deactivate the hold")
    parser.add_option(Option("-n", dest="commit", action="store_false",
        help="do not save the allocation"))
    parser.set_defaults(commit=True)
    return parser


def edit_charge_parser ():
    """An optparse parser for editing existing charges."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option("-c", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    parser.add_option(Option("-n", dest="commit", action="store_false",
        help="do not save changes to the charge"))
    parser.add_option(Option("-D", "--delete",
        dest="delete", action="store_true",
        help="delete the charge"))
    parser.add_option(Option("-A", "--allocation",
        dest="allocation", type="allocation",
        help="move the charge to ALLOCATION", metavar="ALLOCATION"))
    parser.set_defaults(commit=True, delete=False)
    return parser


def edit_refund_parser ():
    """An optparse parser for editing existing refunds."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option("-c", "--comment", dest="comment",
        help="arbitrary COMMENT", metavar="COMMENT")
    parser.add_option(Option("-n", dest="commit", action="store_false",
        help="do not save changes to the refund"))
    parser.add_option(Option("-D", "--delete",
        dest="delete", action="store_true",
        help="delete the refund"))
    parser.set_defaults(commit=True, delete=False)
    return parser


def import_jobs_parser ():
    """An optparse parser for importing jobs."""
    parser = optparse.OptionParser(version=clusterbank.__version__)
    parser.add_option(Option("-v", "--verbose", dest="verbose",
        action="store_true", help="display each imported job"))
    parser.set_defaults(verbose=False)
    return parser


class Option (optparse.Option):
    
    """An extended optparse option with cbank-specific types.
    
    Types:
    date -- parse a datetime from a variety of string formats
    user -- parse a user from its name or id
    project -- parse a project from its name or id
    resource -- parse a resource from its name or id
    job -- parse a job from its id
    allocation -- parser an allocation from its id
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
    
    def check_job (self, opt, value):
        """Parse a job from its id."""
        try:
            return Session.query(Job).filter_by(id=value).one()
        except InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown job: %s" % (opt, value))
    
    def check_allocation (self, opt, value):
        """Parse an allocation from its id."""
        try:
            return Session.query(Allocation).filter_by(id=value).one()
        except InvalidRequestError:
            raise optparse.OptionValueError(
                "option %s: unknown job: %s" % (opt, value))
    
    TYPES = optparse.Option.TYPES + (
        "date", "project", "resource", "user", "job", "allocation")
    
    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER['date'] = check_date
    TYPE_CHECKER['project'] = check_project
    TYPE_CHECKER['resource'] = check_resource
    TYPE_CHECKER['user'] = check_user
    TYPE_CHECKER['job'] = check_job
    TYPE_CHECKER['allocation'] = check_allocation
