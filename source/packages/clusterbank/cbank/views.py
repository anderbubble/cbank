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

import sqlalchemy.sql as sql

from clusterbank import config
from clusterbank.cbank.common import get_unit_factor
import clusterbank.model as model

__all__ = ["print_unit_definition", "print_users_report", "print_projects_report"]

def print_unit_definition ():
    try:
        unit_label = config.get("cbank", "unit_label")
    except ConfigParser.Error:
        return
    unit_definition = "Units are in %s." % unit_label
    print unit_definition

locale.setlocale(locale.LC_ALL, locale.getdefaultlocale()[0])

def print_users_report (**kwargs):
    
    """Users report.
    
    The users report lists the number of charges and total amount charged
    for each user in the system.
    
    Keyword arguments:
    projects -- require project membership and charge relationship
    """
    
    # Set up the table.
    format = Formatter(["User", "Charges", "Charged"])
    format.widths = {'User':10, 'Charges':8, 'Charged':15}
    format.aligns = {'Charges':"right", 'Charged':"right"}
    print format.header()
    print format.bar()
    
    s = model.Session()
    
    relation = model.users_table
    relation = relation.outerjoin(model.charges_table)
    relation = relation.outerjoin(model.refunds_table)
    relation = relation.join(model.allocations_table)
    relation = relation.join(model.projects_table)
    query = sql.select([model.users_table.c.id,
        sql.func.count(model.users_table.c.id).label("charges_count"),
        sql.func.sum(model.charges_table.c.amount).label("charges_sum"),
        sql.func.sum(model.refunds_table.c.amount).label("refunds_sum")])
    query = query.select_from(relation)
    query = query.group_by(model.users_table.c.id)
    
    if kwargs.get("projects"):
        query = query.where(model.projects_table.c.id.in_(
            project.id for project in kwargs.get("projects")))
        members = set(sum([model.project_members(project)
            for project in kwargs.get("projects")], []))
    else:
        members = None
    if kwargs.get("users"):
        users = set(kwargs.get("users"))
        if kwargs.get("projects"):
            users = users & members
    else:
        users = members or []
    query = query.where(model.users_table.c.id.in_(user.id for user in users))
    if kwargs.get("resources"):
        query = query.where(model.resources_table.c.id.in_(
            resource.id for resource in kwargs.get("resources")))
    if kwargs.get("after"):
        query = query.where(
            model.charges_table.c.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        query = query.where(moel.charges_table.c.datetime<kwargs.get("before"))
    
    total_charges_count = 0
    total_charges_sum = 0
    for row in s.execute(query):
        user = model.upstream.get_user_name(row[model.users_table.c.id])
        charges_count = int(row['charges_count'] or 0)
        charges_sum = \
            int(row['charges_sum'] or 0) - int(row['refunds_sum'] or 0)
        total_charges_count += charges_count
        total_charges_sum += charges_sum
        print format({'User':user, 'Charges':charges_count,
            'Charged':display_units(charges_sum)})
    print format.bar(["Charges", "Charged"])
    print format({'Charges':total_charges_count,
        'Charged':display_units(total_charges_sum)})

def print_projects_report (**kwargs):
    
    """Projects report.
    
    The projects report lists allocations and charges for each project
    in the system.
    """
    
    # Set up the table.
    format = Formatter([
        "Project", "Allocated", "Charges", "Charged", "Available"])
    format.widths = {'Project':15, 'Allocated':17,
        'Charges':7, 'Charged':15, 'Available':15}
    format.aligns = {'Allocated':"right", 'Charges':"right",
        'Charged':"right", "Available":"right"}
    print format.header()
    print format.bar()
    
    s = model.Session()
    now = datetime.now()
    
    relation = model.projects_table
    relation = relation.outerjoin(model.allocations_table)
    relation = relation.join(model.resources_table)
    relation = relation.outerjoin(model.charges_table)
    relation = relation.outerjoin(model.refunds_table)
    
    query = sql.select([
        model.projects_table.c.id,
        sql.func.sum(model.allocations_table.c.amount).label("allocations_sum"),
        sql.func.count(model.charges_table.c.id).label("charges_count"),
        sql.func.sum(model.charges_table.c.amount).label("charges_sum"),
        sql.func.sum(model.refunds_table.c.amount).label("refunds_sum")])
    query = query.select_from(relation)
    query = query.group_by(model.projects_table.c.id)
    query = query.where(model.allocations_table.c.start<=now)
    query = query.where(model.allocations_table.c.expiration>now)
    if kwargs.get("projects"):
        query = query.where(model.projects_table.c.id.in_(
            project.id for project in kwargs.get("projects")))
    if kwargs.get("resources"):
        query = query.where(model.resources_table.c.id.in_(
            resource.id for resource in kwargs.get("resources")))
    if kwargs.get("users"):
        query = query.where(model.charges_table.c.user_id.in_(
            user.id for user in kwargs.get("users")))
    if kwargs.get("after"):
        query = query.where(model.charges_table.c.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        query = query.where(model.charges_table.c.datetime<kwargs.get("before"))
    
    total_allocations_sum = 0
    total_charges_count = 0
    total_charges_sum = 0
    for row in s.execute(query):
        project = \
            model.upstream.get_project_name(row[model.projects_table.c.id])
        allocations_sum = int(row['allocations_sum'] or 0)
        charges_count = int(row['charges_count'] or 0)
        charges_sum = \
            int(row['charges_sum'] or 0) - int(row['refunds_sum'] or 0)
        total_allocations_sum += allocations_sum
        total_charges_count += charges_count
        total_charges_sum += charges_sum
        print format({'Project':project,
            'Allocated':display_units(allocations_sum),
            'Charges':charges_count, 'Charged':display_units(charges_sum),
            'Available':display_units(allocations_sum-charges_sum)})
    print format.bar(["Allocated", "Charges", "Charged", "Available"])
    print format({'Allocated':display_units(total_allocations_sum),
        'Charges':total_charges_count,
        'Charged':display_units(total_charges_sum),
        'Available':display_units(total_allocations_sum-total_charges_count)})

def print_member_allocations_report (user, **kwargs):
    allocations = model.Session.query(model.Allocation)
    project_ids = [project.id for project in model.user_projects(user)]
    allocations = allocations.filter(model.Allocation.project.has(model.Project.id.in_(project_ids)))
    print_raw_allocations_report(allocations, **kwargs)

def print_admin_allocations_report (**kwargs):
    allocations = model.Session.query(model.Allocation)
    print_raw_allocations_report(allocations, **kwargs)

def print_raw_allocations_report (allocations_query, **kwargs):
    allocations = allocations_query
    if kwargs.get("projects"):
        project_ids = [project.id for project in kwargs.get("projects")]
        allocations = allocations.filter(model.Allocation.project.has(model.Project.id.in_(project_ids)))
    if kwargs.get("users"):
        project_ids = [project.id for project in sum([model.user_projects(user) for user in kwargs.get("users")], [])]
        allocations = allocations.filter(model.Allocation.project.has(model.Project.id.in_(set(project_ids))))
    if kwargs.get("resources"):
        resource_ids = [resource.id for resource in kwargs.get("resources")]
        allocations = allocations.filter(model.Allocation.resource.has(model.Resource.id.in_(resource_ids)))
    if kwargs.get("after") or kwargs.get("before"):
        if kwargs.get("after"):
            allocations = allocations.filter(model.Allocation.expiration>=kwargs.get("after"))
        if kwargs.get("before"):
            allocations = allocations.filter(model.Allocation.start<kwargs.get("before"))
    else:
        now = datetime.now()
        allocations = allocations.filter(model.Allocation.start<=now)
        allocations = allocations.filter(model.Allocation.expiration>now)
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
    total_allocated = int(allocations.sum(model.Allocation.amount) or 0)
    total_available = sum([allocation.amount_available for allocation in allocations])
    print >> sys.stderr, format(dict(Allocated=display_units(total_allocated), Available=display_units(total_available))), "(total)"
    print_unit_definition()

def print_member_charges_report (user, **kwargs):
    charges = model.Session.query(model.Charge)
    member_project_ids = [project.id for project in model.user_projects(user)]
    owner_project_ids = [project.id for project in model.user_projects_owned(user)]
    charges = charges.filter(sql.or_(
        sql.and_(
            model.Charge.allocation.has(model.Allocation.project.has(model.Project.id.in_(member_project_ids))),
            model.Charge.user==user),
        model.Charge.allocation.has(model.Allocation.project.has(model.Project.id.in_(owner_project_ids)))))
    print_raw_charges_report(charges, **kwargs)

def print_admin_charges_report (**kwargs):
    charges = model.Session.query(model.Charge)
    print_raw_charges_report(charges, **kwargs)

def print_raw_charges_report (charges_query, **kwargs):
    charges = charges_query
    if kwargs.get("projects"):
        project_ids = [project.id for project in kwargs.get("projects")]
        charges = charges.filter(model.Charge.allocation.has(model.Allocation.project.has(model.Project.id.in_(project_ids))))
    if kwargs.get("users"):
        user_ids = [user.id for user in kwargs.get("users")]
        charges = charges.filter(model.Charge.user.has(model.User.id.in_(user_ids)))
    if kwargs.get("resources"):
        resource_ids = [resource.id for resource in kwargs.get("resources")]
        charges = charges.filter(model.Charge.allocation.has(model.Allocation.resource.has(model.Resource.id.in_(resource_ids))))
    if kwargs.get("after"):
        charges = charges.filter(model.Charge.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        charges = charges.filter(model.Charge.datetime<kwargs.get("before"))
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
    total = int(charges.sum(model.Charge.amount) or 0) - int(charges.join("refunds").sum(model.Refund.amount) or 0)
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
    print " * model.Project: '%s'" % allocation.project
    print " * model.Resource: '%s'" % allocation.resource
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
    print " * model.Allocation #%i -- %s (%s available)" % (
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
    print " * model.Charge #%i -- %s (originally %s)" % (
        refund.charge.id, charge_effective_amount,
        charge_amount)


class Formatter (object):
    
    def __init__ (self, fields, **kwargs):
        self.fields = fields
        self.headers = kwargs.get("headers", {}).copy()
        self.widths = kwargs.get("widths", {}).copy()
        self.aligns = kwargs.get("aligns", {}).copy()
    
    def __call__ (self, *args, **kwargs):
        return self.format(*args, **kwargs)
    
    def format (self, data):
        formatted_data = []
        for field in self.fields:
            datum = data.get(field, "")
            datum = str(datum)
            align = self.aligns.get(field)
            width = self.widths.get(field)
            if align or width:
                if align is None:
                    align = "left"
                if width is None:
                    width = 0
                if align == "left":
                    datum = datum.ljust(width)
                elif align == "right":
                    datum = datum.rjust(width)
                elif align == "center":
                    datum = datum.center(width)
                else:
                    raise Exception("Unknown alignment: %s" % align)
            formatted_data.append(datum)
        return " ".join(formatted_data)
    
    def bar (self, fields=None):
        if fields is None:
            fields = self.fields
        bars = {}
        for field in fields:
            bars[field] = "-" * (self.widths.get(field) or 0)
        return self.format(bars)
    
    def header (self, fields=None):
        if fields is None:
            fields = self.fields
        headers = {}
        for field in fields:
            headers[field] = self.headers.get(field, field)
        return self.format(headers)

