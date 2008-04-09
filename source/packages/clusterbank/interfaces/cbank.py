"""cbank part deux"""

import sys
import os
import pwd
from itertools import izip
import string
from ConfigParser import SafeConfigParser as ConfigParser, NoSectionError, NoOptionError
from optparse import OptionParser, Option
from warnings import warn
import locale
from datetime import datetime

from sqlalchemy import or_, and_

import clusterbank
import clusterbank.exceptions
from clusterbank import upstream
from clusterbank.model import User, Project, Allocation, Charge, Refund

locale.setlocale(locale.LC_ALL, locale.getdefaultlocale()[0])

config = ConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    unit_label = config.get("cbank", "unit_label")
except (NoSectionError, NoOptionError):
    unit_definition = None
else:
    unit_definition = "Units are in %s." % unit_label

reports_available = ["projects", "allocations", "charges"]

argv = OptionParser(usage=os.linesep.join([
    "cbank [options] [report]",
    "",
    "reports:",
    "  %s" % ", ".join(reports_available)]), version="cbank %s" % clusterbank.__version__)
argv.add_option(Option("-p", "--project", dest="project",
    help="filter by project NAME", metavar="NAME"))
argv.add_option(Option("-u", "--user", dest="user",
    help="filter by user NAME", metavar="NAME"))
argv.add_option(Option("-r", "--resource", dest="resource",
    help="filter by resource NAME", metavar="NAME"))
try:
    argv.set_defaults(resource=config.get("cbank", "resource"))
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

def main ():
    options, args = argv.parse_args()
    report = handle_exceptions(get_requested_report, args)
    handle_exceptions(run, report, project=options.project, user=options.user, resource=options.resource)

def handle_exceptions (func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except KeyboardInterrupt:
        sys.exit(1)
    except CbankError, e:
        print >> sys.stderr, e
        sys.exit(1)

def run (report, **kwargs):
    if report == "allocations":
        allocations = get_allocations(**kwargs)
        display_allocations(allocations)
    elif report == "projects":
        projects = get_projects(**kwargs)
        display_projects(projects)
    elif report == "charges":
        charges = get_charges(**kwargs)
        display_charges(charges)
    else:
        raise UnknownReport(report)

def get_requested_report (args):
    try:
        requested_report = args[0]
    except IndexError:
        return "projects" # default report type
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

def get_projects (**kwargs):
    user = get_current_user()
    project_ids = [project.id for project in user.projects]
    projects = Project.query.filter(Project.id.in_(project_ids))
    if kwargs.get("project"):
        project_id = upstream.get_project_id(kwargs.get("project"))
        projects = projects.filter_by(id=project_id)
    if kwargs.get("user"):
        user_id = upstream.get_user_id(kwargs.get("user"))
        other_project_ids = upstream.get_member_projects(user_id)
        projects = projects.filter(Project.id.in_(other_project_ids))
    if kwargs.get("resource"):
        resource_id = upstream.get_resource_id(kwargs.get("resource"))
        projects = projects.filter(Project.allocations.any(Allocation.resource.has(id=resource_id)))
    return projects

def get_allocations (**kwargs):
    user = get_current_user()
    project_ids = [project.id for project in user.projects]
    allocations = Allocation.query.filter(Allocation.project.has(Project.id.in_(project_ids)))
    if kwargs.get("project"):
        project_id = upstream.get_project_id(kwargs.get("project"))
        allocations = allocations.filter(Allocation.project.has(id=project_id))
    if kwargs.get("user"):
        user_id = upstream.get_user_id(kwargs.get("user"))
        other_project_ids = upstream.get_member_projects(user_id)
        allocations = allocations.filter(Allocation.project.has(Project.id.in_(other_project_ids)))
    if kwargs.get("resource"):
        resource_id = upstream.get_resource_id(kwargs.get("resource"))
        allocations = allocations.filter(Allocation.resource.has(id=resource_id))
    return allocations

def get_charges (**kwargs):
    user = get_current_user()
    member_project_ids = [project.id for project in user.projects]
    owner_project_ids = [project.id for project in user.projects_owned]
    charges = Charge.query.filter(or_(
        and_(
            Charge.allocation.has(Allocation.project.has(Project.id.in_(member_project_ids))),
            Charge.user==user),
        Charge.allocation.has(Allocation.project.has(Project.id.in_(owner_project_ids)))))
    if kwargs.get("project"):
        project_id = upstream.get_project_id(kwargs.get("project"))
        charges = charges.filter(Charge.allocation.has(Allocation.project.has(id=project_id)))
    if kwargs.get("user"):
        user_id = upstream.get_user_id(kwargs.get("user"))
        charges = charges.filter(Charge.user.has(id=user_id))
    if kwargs.get("resource"):
        resource_id = upstream.get_resource_id(kwargs.get("resource"))
        charges = charges.filter(Charge.allocation.has(Allocation.resource.has(id=resource_id)))
    return charges

def display_projects (projects):
    if not projects.count():
        print >> sys.stderr, "No projects found."
        return
    user = get_current_user()
    format = Formatter([15, 7, 5, (15, string.rjust), (15, string.rjust)])
    print >> sys.stderr, format(["Name", "Members", "Owner", "Allocated", "Available"])
    print >> sys.stderr, format(["-"*15, "-"*7, "-"*5, "-"*15, "-"*15])
    for project in projects:
        if user in project.owners:
            is_owner = "yes"
        else:
            is_owner = "no"
        now = datetime.now()
        allocations = Allocation.query.filter_by(project=project).filter(Allocation.start<=now).filter(Allocation.expiration>now)
        allocated = int(allocations.sum(Allocation.amount) or 0)
        available = allocated - int(allocations.join("charges").sum(Charge.amount) or 0) - int(allocations.join(["charges", "refunds"]).sum(Refund.amount) or 0)
        print format([project.name, len(project.members), is_owner, display_units(allocated), display_units(available)])
    if unit_definition:
        print >> sys.stderr, unit_definition

def display_allocations (allocations):
    if not allocations.count():
        print >> sys.stderr, "No allocations found."
        return
    format = Formatter([10, 10, 15, (15, string.rjust), (15, string.rjust)])
    print >> sys.stderr, format(["Expires", "Resource", "Project", "Allocated", "Available"])
    print >> sys.stderr, format(["-"*10, "-"*10, "-"*15, "-"*15, "-"*15])
    for allocation in allocations:
        print format([allocation.expiration.strftime("%Y-%m-%d"), allocation.resource, allocation.project, display_units(allocation.amount), display_units(allocation.amount_available)])
    if unit_definition:
        print >> sys.stderr, unit_definition

def display_charges (charges):
    if not charges.count():
        print >> sys.stderr, "No charges found."
        return
    format = Formatter([10, 10, 15, (10, string.rjust)])
    print >> sys.stderr, format(["Date", "Resource", "Project", "Amount"])
    print >> sys.stderr, format(["-"*10, "-"*10, "-"*15, "-"*10])
    for charge in charges:
        print format([charge.datetime.strftime("%Y-%m-%d"), charge.allocation.resource, charge.allocation.project, display_units(charge.effective_amount)])
    print >> sys.stderr, format(["", "", "", "-"*10])
    total = display_units(int(charges.sum(Charge.amount) or 0) - int(charges.join("refunds").sum(Refund.amount) or 0))
    print >> sys.stderr, format(["", "", "", total]), "(total)"
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
        assert len(args) == len(self.cols), "Too many arguments to format."
        return self.sep.join([align(str(arg), width) for (arg, (width, align)) in izip(args, self.cols)])

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

