import os
import sys
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

from clusterbank import config
import clusterbank.cbank.exceptions as exceptions
from clusterbank.cbank.common import get_unit_factor
from clusterbank.model import \
    Session, User, Project, Resource, Allocation, Hold, \
    Charge, Refund, user_projects, user_projects_owned, project_owners, project_members
    
def print_unit_definition ():
    try:
        unit_label = config.get("cbank", "unit_label")
    except ConfigParser.Error:
        return
    unit_definition = "Units are in %s." % unit_label
    print unit_definition

locale.setlocale(locale.LC_ALL, locale.getdefaultlocale()[0])

def print_member_usage_report (user, **kwargs):
    projects = Session.query(Project)
    project_ids = [project.id for project in user_projects(user)]
    projects = projects.filter(Project.id.in_(project_ids))
    print_raw_usage_report(projects, **kwargs)

def print_admin_usage_report (**kwargs):
    projects = Session.query(Project)
    print_raw_usage_report(projects, **kwargs)

def print_raw_usage_report (projects_query, **kwargs):
    projects = projects_query
    allocations = Session.query(Allocation)
    charges = Session.query(Charge)
    if kwargs.get("projects"):
        project_ids = [project.id for project in kwargs.get("projects")]
        projects = projects.filter(Project.id.in_(project_ids))
    if kwargs.get("users"):
        project_ids = [project.id for project in sum([user_projects(user) for user in kwargs.get("users")], [])]
        projects = projects.filter(Project.id.in_(project_ids))
    if kwargs.get("resources"):
        resource_ids = [resource.id for resource in kwargs.get("resources")]
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
    print_unit_definition()

def print_member_projects_report (user, **kwargs):
    projects = Session.query(Project)
    project_ids = [project.id for project in user_projects(user)]
    projects = projects.filter(Project.id.in_(project_ids))
    print_raw_projects_report(projects, **kwargs)

def print_admin_projects_report (**kwargs):
    projects = Session.query(Project)
    print_raw_projects_report(projects, **kwargs)

def print_raw_projects_report (projects_query, **kwargs):
    projects = projects_query
    if kwargs.get("projects"):
        project_ids = [project.id for project in kwargs.get("projects")]
        projects = projects.filter(Project.id.in_(project_ids))
    if kwargs.get("users"):
        project_ids = [project.id for project in sum([user_projects(user) for user in kwargs.get("users")], [])]
        projects = projects.filter(Project.id.in_(project_ids))
    if kwargs.get("resources"):
        resource_ids = [resource.id for resource in kwargs.get("resources")]
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
    print_unit_definition()

def print_member_allocations_report (user, **kwargs):
    allocations = Session.query(Allocation)
    project_ids = [project.id for project in user_projects(user)]
    allocations = allocations.filter(Allocation.project.has(Project.id.in_(project_ids)))
    print_raw_allocations_report(allocations, **kwargs)

def print_admin_allocations_report (**kwargs):
    allocations = Session.query(Allocation)
    print_raw_allocations_report(allocations, **kwargs)

def print_raw_allocations_report (allocations_query, **kwargs):
    allocations = allocations_query
    if kwargs.get("projects"):
        project_ids = [project.id for project in kwargs.get("projects")]
        allocations = allocations.filter(Allocation.project.has(Project.id.in_(project_ids)))
    if kwargs.get("users"):
        project_ids = [project.id for project in sum([user_projects(user) for user in kwargs.get("users")], [])]
        allocations = allocations.filter(Allocation.project.has(Project.id.in_(set(project_ids))))
    if kwargs.get("resources"):
        resource_ids = [resource.id for resource in kwargs.get("resources")]
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
    print_unit_definition()

def print_member_charges_report (user, **kwargs):
    charges = Session.query(Charge)
    member_project_ids = [project.id for project in user_projects(user)]
    owner_project_ids = [project.id for project in user_projects_owned(user)]
    charges = charges.filter(or_(
        and_(
            Charge.allocation.has(Allocation.project.has(Project.id.in_(member_project_ids))),
            Charge.user==user),
        Charge.allocation.has(Allocation.project.has(Project.id.in_(owner_project_ids)))))
    print_raw_charges_report(charges, **kwargs)

def print_admin_charges_report (**kwargs):
    charges = Session.query(Charge)
    print_raw_charges_report(charges, **kwargs)

def print_raw_charges_report (charges_query, **kwargs):
    charges = charges_query
    if kwargs.get("projects"):
        project_ids = [project.id for project in kwargs.get("projects")]
        charges = charges.filter(Charge.allocation.has(Allocation.project.has(Project.id.in_(project_ids))))
    if kwargs.get("users"):
        user_ids = [user.id for user in kwargs.get("users")]
        charges = charges.filter(Charge.user.has(User.id.in_(user_ids)))
    if kwargs.get("resources"):
        resource_ids = [resource.id for resource in kwargs.get("resources")]
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
    print_unit_definition()

def display_units (amount):
    mul, div = get_unit_factor()
    converted_amount = amount * mul / div
    if 0 < converted_amount < 0.1:
        return "< 0.1"
    else:
        return locale.format("%.1f", converted_amount, True)

def print_allocation (allocation):
    amount = display_units(allocation.amount)
    amount_available = display_units(allocation.amount_available)
    allocation_str = "Allocation #%i -- %s (%s available)" % (
        allocation.id, amount, amount_available)
    if allocation.comment:
        allocation_str = " ".join([allocation_str, "(%s)" % allocation.comment])
    print allocation_str
    print " * Project: '%s'" % allocation.project
    print " * Resource: '%s'" % allocation.resource
    print " * Start: %s" % allocation.start
    print " * Expiration: %s" % allocation.expiration

def print_charges (charges):
    for charge in charges:
        print_charge(charge)

def print_charge (charge):
    effective_amount = display_units(charge.effective_amount)
    amount = display_units(charge.amount)
    charge_str = "Charge #%i -- %s" % (charge.id, effective_amount)
    if amount != effective_amount:
        charge_str = " ".join([charge_str, "(originally %s)" % amount])
    if charge.comment:
        charge_str = " ".join([charge_str, "(%s)" % charge.comment])
    print charge_str
    print " * Allocation #%i -- %s (%s available)" % (
        charge.allocation.id, charge.allocation.amount,
        charge.allocation.amount_available)

def print_refund (refund):
    amount = display_units(refund.amount)
    refund_str = "Refund #%i -- %s" % (refund.id, amount)
    if refund.comment:
        refund_str = " ".join([refund_str, "(%s)" % refund.comment])
    print refund_str
    charge_effective_amount = display_units(refund.charge.effective_amount)
    charge_amount = display_units(refund.charge.amount)
    print " * Charge #%i -- %s (originally %s)" % (
        refund.charge.id, charge_effective_amount,
        charge_amount)


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
