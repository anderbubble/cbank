"""cbank part deux"""

import sys
import os
import pwd
import string
import ConfigParser
import optparse
import warnings
import locale
from datetime import datetime
try:
    dt_strptime = datetime.strprime
except AttributeError:
    import time
    def dt_strptime (value, format):
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
                return dt_strptime(value, format)
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

config = ConfigParser.SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    unit_label = config.get("cbank", "unit_label")
except ConfigParser.Error:
    unit_definition = None
else:
    unit_definition = "Units are in %s." % unit_label
try:
    admins = config.get("cbank", "admins")
except ConfigParser.Error:
    admins = []
else:
    admins = admins.split(",")

reports_available = ["use", "usage", "projects", "allocations", "charges"]

parser = optparse.OptionParser(usage=os.linesep.join([
    "cbank [options] [report]",
    "",
    "reports:",
    "  %s" % ", ".join(reports_available)]), version="cbank %s" % clusterbank.__version__)
parser.add_option(Option("-p", "--projects", dest="projects", type="csv",
    help="filter by project NAMES", metavar="NAMES"))
parser.add_option(Option("-u", "--users", dest="users", type="csv",
    help="filter by user NAMES", metavar="NAMES"))
parser.add_option(Option("-r", "--resources", dest="resources", type="csv",
    help="filter by resource NAMES", metavar="NAMES"))
parser.add_option(Option("-a", "--after", dest="after", type="date",
    help="filter by start DATE", metavar="DATE"))
parser.add_option(Option("-b", "--before", dest="before", type="date",
    help="filter by end DATE", metavar="DATE"))
parser.add_option(Option("-e", "--extra-data", dest="extra", action="store_true",
    help="display extra data"))
parser.set_defaults(extra=False)
try:
    parser.set_defaults(resources=config.get("cbank", "resource"))
except ConfigParser.Error:
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

def handle_exceptions (func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except KeyboardInterrupt:
        sys.exit(1)
    except CbankError, e:
        print >> sys.stderr, e
        sys.exit(1)

def main ():
    return handle_exceptions(_main)

def _main ():
    options, args = parser.parse_args()
    report = get_requested_report(args)
    print_report(report, projects=options.projects,
        users=options.users, resources=options.resources, after=options.after,
        before=options.before, extra=options.extra)

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

def print_report (report, **kwargs):
    if report in ("use", "usage"):
        print_usage(**kwargs)
    elif report == "projects":
        print_projects(**kwargs)
    elif report == "allocations":
        allocations = get_allocations(**kwargs)
        display_allocations(allocations, extra=extra)
    elif report == "charges":
        charges = get_charges(**kwargs)
        display_charges(charges, extra=extra)
    else:
        raise UnknownReport(report)

def print_usage (**kwargs):
    user = get_current_user()
    projects = Project.query()
    allocations = Allocation.query()
    charges = Charge.query()
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
    format = Formatter(["Project", "Allocated", "Used", "Balance"])
    format.widths = dict.fromkeys(format.fields, 15)
    format.aligns = dict(Allocated=string.rjust, Used=string.rjust, Balance=string.rjust)
    print >> sys.stderr, format.header
    print >> sys.stderr, format.bar
    total_allocated, total_used = 0, 0
    for project in projects:
        project_allocations = allocations.filter_by(project=project)
        project_charges = charges.filter(Charge.allocation.has(project=project))
        allocation_amount = int(project_allocations.sum(Allocation.amount) or 0)
        total_allocated += allocation_amount
        charge_amount = int(project_charges.sum(Charge.amount) or 0)
        refund_amount = int(project_charges.join(Charge.refunds).sum(Refund.amount) or 0)
        used_amount = charge_amount - refund_amount
        total_used += used_amount
        balance = allocation_amount - used_amount
        print format(Project=project.name,
            Allocated=display_units(allocation_amount),
            Used=display_units(used_amount), Balance=display_units(balance))
    print >> sys.stderr, format.bar
    total_balance = total_allocated - total_used
    print >> sys.stderr, format(Allocated=display_units(total_allocated), Used=display_units(total_used), Balance=display_units(total_balance)), "(total)"
    if unit_definition:
        print unit_definition

def print_projects (**kwargs):
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
    if not projects.count():
        print >> sys.stderr, "No projects found."
        return
    user = get_current_user()
    format = Formatter(["Name", "Members", "Owner"])
    format.widths = dict(Name=15, Members=7, Owner=5)
    print >> sys.stderr, format.header
    print >> sys.stderr, format.bar
    for project in projects:
        if user in project.owners:
            is_owner = "yes"
        else:
            is_owner = "no"
        print format(Name=project.name, Members=len(project.members), Owner=is_owner)
    if unit_definition:
        print unit_definition

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
    format = OldFormatter(widths)
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
    format = OldFormatter(widths)
    print >> sys.stderr, format(header)
    print >> sys.stderr, format.linesep()
    for charge in charges:
        print format(get_data(charge))
    print >> sys.stderr, format.linesep(header.index("Amount"))
    total = int(charges.sum(Charge.amount) or 0) - int(charges.join("refunds").sum(Refund.amount) or 0)
    print >> sys.stderr, format([""]*header.index("Amount") + [display_units(total)]), "(total)"
    if unit_definition:
        print >> sys.stderr, unit_definition

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

def display_units (amount):
    try:
        factor = config.get("cbank", "unit_factor")
    except ConfigParser.Error:
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
        warnings.warn("invalid unit factor: %s" % factor)
        mul = 1
        val = 1
    converted_amount = amount * mul / div
    if 0 < converted_amount < 0.1:
        return "< 0.1"
    else:
        return locale.format("%.1f", converted_amount, True)

class Formatter (object):
    
    def __init__ (self, fields, **kwargs):
        self.fields = fields
        self.sep = kwargs.get("sep", " ")
        self.barchar = kwargs.get("barchar", "-")
        self.headers = dict(**kwargs.get("headers", {}))
        self.widths = dict(**kwargs.get("widths", {}))
        self.aligns = dict(**kwargs.get("aligns", {}))
        self.mods = dict(**kwargs.get("mods", {}))
    
    def _get_bar (self):
        return self.format(**dict((field, self.barchar*self._width(field)) for field in self.fields))
    
    bar = property(_get_bar)

    def _get_header (self):
        mods = self.mods
        try:
            self.mods = {}
            return self.format(**dict((field, self._field_header(field)) for field in self.fields))
        finally:
            self.mods = mods
    
    header = property(_get_header)
    
    def _field_header (self, field): 
        return self.headers.get(field, field)
    
    def _width (self, field):
        try:
            return self.widths[field]
        except KeyError:
            return max(len(line) for line in self._field_header(field).split(os.linesep))
    
    def _align (self, field, value):
        return self.aligns.get(field, string.ljust)(value, self._width(field))
    
    def _mod (self, field, value):
        try:
            return self.mods.get(field, "%s") % value
        except TypeError:
            return str(value)
    
    def format (self, **data):
        formatted_data = []
        for field in self.fields:
            data_item = data.get(field, "")
            data_item = self._mod(field, data_item)
            data_item = self._align(field, data_item)
            formatted_data.append(data_item)
        return self.sep.join(formatted_data)
    
    def __call__ (self, *args, **kwargs):
        return self.format(*args, **kwargs)

class OldFormatter (object):
    
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

