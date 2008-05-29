"""cbank part deux"""

import sys
import os
import pwd
import string
from ConfigParser import SafeConfigParser as ConfigParser, NoSectionError, NoOptionError
import optparse
from optparse import OptionParser
from warnings import warn
import locale
from datetime import datetime
try:
    strptime = datetime.strprime
except AttributeError:
    import time
    def strptime (value, format):
        return datetime(*time.strptime(value, format)[0:6])
try:
    set
except NameError:
    from sets import Set as set

from sqlalchemy import or_, and_

import clusterbank
import clusterbank.exceptions
from clusterbank import upstream
from clusterbank.model import User, Project, Resource, Allocation, Hold, Charge, Refund

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
                return strptime(value, format)
            except ValueError:
                continue
        raise optparse.OptionValueError(
            "option %s: invalid date: %r" % (opt, value))
    
    def check_csv (self, opt, value):
        if value:
            return value.split(",")
        else:
            return []
    
    TYPES = optparse.Option.TYPES + ("date", "csv")
    
    TYPE_CHECKER = optparse.Option.TYPE_CHECKER.copy()
    TYPE_CHECKER['date'] = check_date
    TYPE_CHECKER['csv'] = check_csv

locale.setlocale(locale.LC_ALL, locale.getdefaultlocale()[0])

config = ConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    unit_label = config.get("cbank", "unit_label")
except (NoSectionError, NoOptionError):
    unit_definition = None
else:
    unit_definition = "Units are in %s." % unit_label
try:
    admins = config.get("cbank", "admins")
except (NoSectionError, NoOptionError):
    admins = []
else:
    admins = admins.split(",")

reports_available = ["use", "usage", "projects", "allocations", "charges"]

argv = OptionParser(usage=os.linesep.join([
    "cbank [options] [report]",
    "",
    "reports:",
    "  %s" % ", ".join(reports_available)]), version="cbank %s" % clusterbank.__version__)
argv.add_option(Option("-p", "--projects", dest="projects", type="csv",
    help="filter by project NAMES", metavar="NAMES"))
argv.add_option(Option("-u", "--users", dest="users", type="csv",
    help="filter by user NAMES", metavar="NAMES"))
argv.add_option(Option("-r", "--resources", dest="resources", type="csv",
    help="filter by resource NAMES", metavar="NAMES"))
argv.add_option(Option("-a", "--after", dest="after", type="date",
    help="filter by start DATE", metavar="DATE"))
argv.add_option(Option("-b", "--before", dest="before", type="date",
    help="filter by end DATE", metavar="DATE"))
argv.add_option(Option("-e", "--extra-data", dest="extra", action="store_true",
    help="display extra data"))
argv.set_defaults(extra=False)
try:
    argv.set_defaults(resources=config.get("cbank", "resource"))
except (NoSectionError, NoOptionError):
    pass

class CbankException (Exception): pass

class CbankError (CbankException): pass

class UnknownUser (CbankError):
    
    def __str__ (self):
        return "cbank: unknown user: %s" % CbankError.__str__(self)

class UnknownReport (CbankError):
    
    def __str__ (self):
        return "cbank: unknown report: %s" % CbankError.__str__(self)

class MisusedOption (CbankError):
    
    def __str__ (self):
        return "cbank: misused option: %s" % CbankError.__str__(self)

def main ():
    options, args = argv.parse_args()
    report = handle_exceptions(get_requested_report, args)
    handle_exceptions(run, report, projects=options.projects,
        users=options.users, resources=options.resources, after=options.after,
        before=options.before, extra=options.extra)

def handle_exceptions (func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except KeyboardInterrupt:
        sys.exit(1)
    except CbankError, e:
        print >> sys.stderr, e
        sys.exit(1)

def run (report, extra=False, **kwargs):
    if report in ("use", "usage"):
        usage = get_usage(**kwargs)
        if extra:
            raise MisusedOption("usage report has no extra data")
        display_usage(usage)
    elif report == "projects":
        projects = get_projects(**kwargs)
        if extra:
            raise MisusedOption("project report has no extra data")
        display_projects(projects)
    elif report == "allocations":
        allocations = get_allocations(**kwargs)
        display_allocations(allocations, extra=extra)
    elif report == "charges":
        charges = get_charges(**kwargs)
        display_charges(charges, extra=extra)
    else:
        raise UnknownReport(report)

def get_requested_report (args):
    try:
        requested_report = args[0]
    except IndexError:
        return "usage" # default report type
    else:
        possible_reports = [
            report for report in reports_available
            if report.startswith(requested_report)]
        if not possible_reports:
            raise UnknownReport(requested_report)
        elif len(possible_reports) > 1:
            raise UnknownReport("could be %s" % ", ".join(possible_reports))
        else:
            return possible_reports[0]

def get_usage (**kwargs):
    user = get_current_user()
    
    projects = Project.query()
    if user.name not in admins:
        project_ids = [project.id for project in user.projects]
        projects = projects.filter(Project.id.in_(project_ids))
    if kwargs.get("projects"):
        project_ids = [upstream.get_project_id(project) for project in kwargs.get("projects")]
        projects = projects.filter(Project.id.in_(project_ids))
    if kwargs.get("users"):
        user_ids = [upstream.get_user_id(user) for user in kwargs.get("users")]
        project_ids = set(sum([upstream.get_member_projects(user_id) for user_id in user_ids], []))
        projects = projects.filter(Project.id.in_(project_ids))
    if kwargs.get("resources"):
        resource_ids = [upstream.get_resource_id(resource) for resource in kwargs.get("resources")]
        projects = projects.filter(Project.allocations.any(Allocation.resource.has(Resource.id.in_(resource_ids))))
    
    allocations = Allocation.query()
    charges = Charge.query()
    if kwargs.get("resources"):
        resource_ids = [upstream.get_resource_id(resource) for resource in kwargs.get("resources")]
        allocations = allocations.filter(Allocation.resource.has(Resource.id.in_(resource_ids)))
        charges = charges.filter(Charge.allocation.has(Allocation.resource.has(Resource.id.in_(resource_ids))))
    if kwargs.get("after") or kwargs.get("before"):
        if kwargs.get("after"):
            allocations = allocations.filter(Allocation.expiration>kwargs.get("after"))
            charges = charges.filter(Charge.datetime>=kwargs.get("after"))
        if kwargs.get("before"):
            allocations = allocations.filter(Allocation.start<=kwargs.get("before"))
            charges = charges.filter(Charge.datetime<kwargs.get("before"))
    else:
        now = datetime.now()
        allocations = allocations.filter(and_(Allocation.start<=now, Allocation.expiration>now))
        charges = charges.filter(Charge.allocation.has(and_(Allocation.start<=now, Allocation.expiration>now)))
    
    return ((project, allocations.filter_by(project=project), charges.filter(Charge.allocation.has(project=project))) for project in projects)

def get_projects (**kwargs):
    user = get_current_user()
    projects = Project.query()
    if user.name not in admins:
        project_ids = [project.id for project in user.projects]
        projects = projects.filter(Project.id.in_(project_ids))
    if kwargs.get("projects"):
        project_ids = [upstream.get_project_id(project) for project in kwargs.get("projects")]
        projects = projects.filter(Project.id.in_(project_ids))
    if kwargs.get("users"):
        user_ids = [upstream.get_user_id(user) for user in kwargs.get("users")]
        project_ids = set(sum([upstream.get_member_projects(user_id) for user_id in user_ids], []))
        projects = projects.filter(Project.id.in_(project_ids))
    if kwargs.get("resources"):
        resource_ids = [upstream.get_resource_id(resource) for resource in kwargs.get("resources")]
        projects = projects.filter(Project.allocations.any(Allocation.resource.has(Resource.id.in_(resource_ids))))
    if kwargs.get("after"):
        projects = projects.filter(or_(
            Project.allocations.any(Allocation.datetime>=kwargs.get("after")),
            Project.allocations.any(Allocation.holds.any(Hold.datetime>=kwargs.get("after"))),
            Project.allocations.any(Allocation.charges.any(Charge.datetime>=kwargs.get("after"))),
            Project.allocations.any(Allocation.charges.any(Charge.refunds.any(Refund.datetime>=kwargs.get("after"))))))
    if kwargs.get("before"):
        projects = projects.filter(or_(
            Project.allocations.any(Allocation.datetime<kwargs.get("before")),
            Project.allocations.any(Allocation.holds.any(Hold.datetime<kwargs.get("before"))),
            Project.allocations.any(Allocation.charges.any(Charge.datetime<kwargs.get("before"))),
            Project.allocations.any(Allocation.charges.any(Charge.refunds.any(Refund.datetime<kwargs.get("before"))))))
    return projects

def get_allocations (**kwargs):
    user = get_current_user()
    allocations = Allocation.query()
    if user.name not in admins:
        project_ids = [project.id for project in user.projects]
        allocations = allocations.filter(Allocation.project.has(Project.id.in_(project_ids)))
    if kwargs.get("projects"):
        project_ids = [upstream.get_project_id(project) for project in kwargs.get("projects")]
        allocations = allocations.filter(Allocation.project.has(Project.id.in_(project_ids)))
    if kwargs.get("users"):
        user_ids = [upstream.get_user_id(user) for user in kwargs.get("users")]
        project_ids = sum([upstream.get_member_projects(user_id) for user_id in user_ids], [])
        allocations = allocations.filter(Allocation.project.has(Project.id.in_(set(project_ids))))
    if kwargs.get("resources"):
        resource_ids = [upstream.get_resource_id(resource) for resource in kwargs.get("resources")]
        allocations = allocations.filter(Allocation.resource.has(Resource.id.in_(resource_ids)))
    if kwargs.get("after") or kwargs.get("before"):
        if kwargs.get("after"):
            allocations = allocations.filter(Allocation.expiration>=kwargs.get("after"))
        if kwargs.get("before"):
            allocations = allocations.filter(Allocation.start<kwargs.get("before"))
    else:
        now = datetime.now()
        allocations = allocations.filter(Allocation.start<=now)
        allocations = allocations.filter(Allocation.expiration>now)
    return allocations

def get_charges (**kwargs):
    user = get_current_user()
    charges = Charge.query()
    if user.name not in admins:
        member_project_ids = [project.id for project in user.projects]
        owner_project_ids = [project.id for project in user.projects_owned]
        charges = charges.filter(or_(
            and_(
                Charge.allocation.has(Allocation.project.has(Project.id.in_(member_project_ids))),
                Charge.user==user),
            Charge.allocation.has(Allocation.project.has(Project.id.in_(owner_project_ids)))))
    if kwargs.get("projects"):
        project_ids = [upstream.get_project_id(project) for project in kwargs.get("projects")]
        charges = charges.filter(Charge.allocation.has(Allocation.project.has(Project.id.in_(project_ids))))
    if kwargs.get("users"):
        user_ids = [upstream.get_user_id(user) for user in kwargs.get("users")]
        charges = charges.filter(Charge.user.has(User.id.in_(user_ids)))
    if kwargs.get("resources"):
        resource_ids = [upstream.get_resource_id(resource) for resource in kwargs.get("resources")]
        charges = charges.filter(Charge.allocation.has(Allocation.resource.has(Resource.id.in_(resource_ids))))
    if kwargs.get("after"):
        charges = charges.filter(Charge.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        charges = charges.filter(Charge.datetime<kwargs.get("before"))
    return charges

def display_usage (usage):
    header = ["Project", "Allocated", "Used", "Balance"]
    format = Formatter([15, (15, string.rjust), (15, string.rjust), (15, string.rjust)])
    print >> sys.stderr, format(header)
    print >> sys.stderr, format.linesep()
    total_allocated, total_used = 0, 0
    for project, allocations, charges in usage:
        allocation_amount = int(allocations.sum(Allocation.amount) or 0)
        total_allocated += allocation_amount
        charge_amount = int(charges.sum(Charge.amount) or 0)
        refund_amount = int(charges.join(Charge.refunds).sum(Refund.amount) or 0)
        used_amount = charge_amount - refund_amount
        total_used += used_amount
        balance = allocation_amount - used_amount
        print format([project.name, display_units(allocation_amount), display_units(used_amount), display_units(balance)])
    print >> sys.stderr, format.linesep(header.index("Allocated"), header.index("Used"), header.index("Balance"))
    total_balance = total_allocated - total_used
    print >> sys.stderr, format(["", display_units(total_allocated), display_units(total_used), display_units(total_balance)]), "(total)"
    if unit_definition:
        print >> sys.stderr, unit_definition

def display_projects (projects):
    if not projects.count():
        print >> sys.stderr, "No projects found."
        return
    user = get_current_user()
    format = Formatter([15, 7, 5])
    print >> sys.stderr, format(["Name", "Members", "Owner"])
    print >> sys.stderr, format.linesep()
    for project in projects:
        if user in project.owners:
            is_owner = "yes"
        else:
            is_owner = "no"
        print format([project.name, len(project.members), is_owner])
    if unit_definition:
        print >> sys.stderr, unit_definition

def display_allocations (allocations, extra=False):
    if not allocations.count():
        print >> sys.stderr, "No allocations found."
        return
    if extra:
        widths = [10, 10, 10, 15, (15, string.rjust), (15, string.rjust), 7]
        header = ["Starts", "Expires", "Resource", "Project", "Allocated", "Available", "Comment"]
        def get_data (allocation):
            return [allocation.start.strftime("%Y-%m-%d"), allocation.expiration.strftime("%Y-%m-%d"), allocation.resource, allocation.project, display_units(allocation.amount), display_units(allocation.amount_available), allocation.comment]
    else:
        widths = [10, 10, 15, (15, string.rjust), (15, string.rjust)]
        header = ["Expires", "Resource", "Project", "Allocated", "Available"]
        def get_data (allocation):
            return [allocation.expiration.strftime("%Y-%m-%d"), allocation.resource, allocation.project, display_units(allocation.amount), display_units(allocation.amount_available)]
    format = Formatter(widths)
    print >> sys.stderr, format(header)
    print >> sys.stderr, format.linesep()
    for allocation in allocations:
        print format(get_data(allocation))
    print >> sys.stderr, format.linesep(header.index("Allocated"), header.index("Available"))
    total_allocated = int(allocations.sum(Allocation.amount) or 0)
    total_available = sum([allocation.amount_available for allocation in allocations])
    print >> sys.stderr, format([""]*header.index("Allocated") + [display_units(total_allocated), display_units(total_available)]), "(total)"
    if unit_definition:
        print >> sys.stderr, unit_definition

def display_charges (charges, extra=False):
    if not charges.count():
        print >> sys.stderr, "No charges found."
        return
    if extra:
        widths = [10, 10, 15, 8, (15, string.rjust), 7]
        header = ["Date", "Resource", "Project", "User", "Amount", "Comment"]
        def get_data (charge):
            return [charge.datetime.strftime("%Y-%m-%d"), charge.allocation.resource, charge.allocation.project, charge.user, display_units(charge.effective_amount), charge.comment]
    else:
        widths = [10, 10, 15, 8, (15, string.rjust)]
        header = ["Date", "Resource", "Project", "User", "Amount"]
        def get_data (charge):
            return [charge.datetime.strftime("%Y-%m-%d"), charge.allocation.resource, charge.allocation.project, charge.user, display_units(charge.effective_amount)]
    format = Formatter(widths)
    print >> sys.stderr, format(header)
    print >> sys.stderr, format.linesep()
    for charge in charges:
        print format(get_data(charge))
    print >> sys.stderr, format.linesep(header.index("Amount"))
    total = int(charges.sum(Charge.amount) or 0) - int(charges.join("refunds").sum(Refund.amount) or 0)
    print >> sys.stderr, format([""]*header.index("Amount") + [display_units(total)]), "(total)"
    if unit_definition:
        print >> sys.stderr, unit_definition

def display_units (amount):
    try:
        factor = config.get("cbank", "unit_factor")
    except (NoSectionError, NoOptionError):
        factor = 1
    try:
        mul, div = factor.split("/")
    except ValueError:
        mul = factor
        div = 1
    try:
        mul = float(mul)
        div = float(div)
    except ValueError:
        warn("invalid unit factor: %s" % factor)
        mul = 1
        val = 1
    converted_amount = amount * mul / div
    if 0 < converted_amount < 0.1:
        return "< 0.1"
    else:
        return locale.format("%.1f", converted_amount, True)

class Formatter (object):
    
    def __init__ (self, cols, sep=" "):
        self.cols = [self._with_alignment(col) for col in cols]
        self.sep = sep
    
    @staticmethod
    def _with_alignment (col):
        try:
            width, alignment = col
        except TypeError:
            width = col
            alignment = string.ljust
        return (width, alignment)
    
    def __call__ (self, *args, **kwargs):
        return self.format(*args, **kwargs)
    
    def format (self, args):
        assert len(args) <= len(self.cols), "Too many arguments to format."
        return self.sep.join([align(str(arg), width) for (arg, (width, align)) in zip(args, self.cols)])
    
    def linesep (self, *args):
        if not args:
            cols = range(len(self.cols))
        else:
            cols = args
        return self.format([((x in cols) and "-" or " ")*width for ((width, align), x) in zip(self.cols, range(len(self.cols)))])

def get_current_user ():
    uid = os.getuid()
    try:
        passwd_entry = pwd.getpwuid(uid)
    except KeyError:
        raise UnknownUser("Unable to determine the current user.")
    username = passwd_entry[0]
    try:
        user = User.by_name(username)
    except clusterbank.exceptions.NotFound:
        raise UnknownUser("User '%s' was not found." % username)
    return user
