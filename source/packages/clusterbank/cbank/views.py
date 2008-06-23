import os
import sys
import pwd
import locale
import ConfigParser
import string
import warnings
from datetime import datetime
try:
    set
except NameError:
    from sets import Set as set

from sqlalchemy import or_, and_

import clusterbank
import clusterbank.exceptions
import clusterbank.cbank.exceptions as exceptions
from clusterbank.model import \
    upstream, Session, User, Project, Resource, Allocation, Hold, \
    Charge, Refund, user_by_name, user_projects, user_projects_owned, project_owners, project_members
    
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

locale.setlocale(locale.LC_ALL, locale.getdefaultlocale()[0])

def print_usage (**kwargs):
    user = get_current_user()
    projects = Session.query(Project)
    allocations = Session.query(Allocation)
    charges = Session.query(Charge)
    if user.name not in admins:
        project_ids = [project.id for project in user_projects(user)]
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
        print format(dict(Project=project.name,
            Allocated=display_units(allocation_amount),
            Used=display_units(used_amount), Balance=display_units(balance)))
    print >> sys.stderr, format.bar
    total_balance = total_allocated - total_used
    print >> sys.stderr, format(dict(Allocated=display_units(total_allocated), Used=display_units(total_used), Balance=display_units(total_balance))), "(total)"
    if unit_definition:
        print unit_definition

def print_projects (**kwargs):
    user = get_current_user()
    projects = Session.query(Project)
    if user.name not in admins:
        project_ids = [project.id for project in user_projects(user)]
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
        if user in project_owners(project):
            is_owner = "yes"
        else:
            is_owner = "no"
        print format(dict(Name=project.name, Members=len(project_members(project)), Owner=is_owner))
    if unit_definition:
        print unit_definition

def print_allocations (**kwargs):
    user = get_current_user()
    allocations = Session.query(Allocation)
    if user.name not in admins:
        project_ids = [project.id for project in user_projects(user)]
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
    if not allocations.count():
        print >> sys.stderr, "No allocations found."
        return
    if kwargs.get("extra"):
        format = Formatter(["Starts", "Expires", "Resource", "Project", "Allocated", "Available", "Comment"])
    else:
        format = Formatter(["Expires", "Resource", "Project", "Allocated", "Available"])
    format.widths = dict(Starts=10, Expires=10, Resource=10, Project=15, Allocated=15, Available=15, Comment=7)
    format.aligns = dict(Allocated=string.rjust, Available=string.rjust)
    print >> sys.stderr, format.header
    print >> sys.stderr, format.bar
    for allocation in allocations:
        print format(dict(Starts=allocation.start.strftime("%Y-%m-%d"),
            Expires=allocation.expiration.strftime("%Y-%m-%d"),
            Resource=allocation.resource, Project=allocation.project,
            Allocated=display_units(allocation.amount),
            Available=display_units(allocation.amount_available),
            Comment=allocation.comment))
    print >> sys.stderr, format.bar
    total_allocated = int(allocations.sum(Allocation.amount) or 0)
    total_available = sum([allocation.amount_available for allocation in allocations])
    print >> sys.stderr, format(dict(Allocated=display_units(total_allocated), Available=display_units(total_available))), "(total)"
    if unit_definition:
        print >> sys.stderr, unit_definition

def print_charges (**kwargs):
    user = get_current_user()
    charges = Session.query(Charge)
    if user.name not in admins:
        member_project_ids = [project.id for project in user_projects(user)]
        owner_project_ids = [project.id for project in user_projects_owned(user)]
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
    if not charges.count():
        print >> sys.stderr, "No charges found."
        return
    if kwargs.get("extra"):
        format = Formatter(["Date", "Resource", "Project", "User", "Amount", "Comment"])
    else:
        format = Formatter(["Date", "Resource", "Project", "User", "Amount"])
    format.widths = dict(Date=10, Resource=10, Project=15, User=8, Amount=15, Comment=7)
    format.aligns = dict(Amount=string.rjust)
    print >> sys.stderr, format.header
    print >> sys.stderr, format.bar
    for charge in charges:
        print format(dict(Date=charge.datetime.strftime("%Y-%m-%d"),
            Resource=charge.allocation.resource,
            Project=charge.allocation.project, User=charge.user,
            Amount=display_units(charge.effective_amount),
            Comment=charge.comment))
    print >> sys.stderr, format.bar
    total = int(charges.sum(Charge.amount) or 0) - int(charges.join("refunds").sum(Refund.amount) or 0)
    print >> sys.stderr, format(dict(Amount=display_units(total))), "(total)"
    if unit_definition:
        print >> sys.stderr, unit_definition

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
        self.headers = kwargs.get("headers", {}).copy()
        self.widths = kwargs.get("widths", {}).copy()
        self.aligns = kwargs.get("aligns", {}).copy()
        self.mods = kwargs.get("mods", {}).copy()
    
    def _get_bar (self):
        return self.format(dict((field, self.barchar*self._width(field)) for field in self.fields))
    
    bar = property(_get_bar)

    def _get_header (self):
        mods = self.mods
        try:
            self.mods = {}
            return self.format(dict((field, self._field_header(field)) for field in self.fields))
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
    
    def format (self, data):
        formatted_data = []
        for field in self.fields:
            data_item = data.get(field, "")
            data_item = self._mod(field, data_item)
            data_item = self._align(field, data_item)
            formatted_data.append(data_item)
        return self.sep.join(formatted_data)
    
    def __call__ (self, *args, **kwargs):
        return self.format(*args, **kwargs)
