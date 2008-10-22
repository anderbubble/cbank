import locale
import ConfigParser
from datetime import datetime

from sqlalchemy.sql import and_, cast, func, select
from sqlalchemy.types import Integer, String, TypeDecorator
from sqlalchemy.orm import eagerload

from clusterbank import config
from clusterbank.cbank.common import get_unit_factor
from clusterbank.model import Session, upstream, User, Project, Resource, \
    Allocation, Hold, Charge, Refund

__all__ = ["unit_definition", "convert_units", "display_units",
    "print_users_report", "print_projects_report", "print_allocations_report",
    "print_holds_report", "print_charges_report"]

locale.setlocale(locale.LC_ALL, locale.getdefaultlocale()[0])


def print_users_report (users, projects=None, resources=None,
                        after=None, before=None):
    
    """Users report.
    
    The users report lists the number of charges and total amount charged
    for each specified user.
    """
    
    format = Formatter(["Name", "Charges", "Charged"])
    format.widths = {'Name':10, 'Charges':8, 'Charged':15}
    format.aligns = {'Charges':"right", 'Charged':"right"}
    print format.header()
    print format.bar()
    
    s = Session()
    charges_q = s.query(
        User.id.label("user_id"),
        func.count(Charge.id).label("charge_count"),
        func.sum(Charge.amount).label("charge_sum")).group_by(User.id)
    charges_q = charges_q.join(Charge.user)
    refunds_q = s.query(
        User.id.label("user_id"),
        func.sum(Refund.amount).label("refund_sum")).group_by(User.id)
    refunds_q = refunds_q.join(Refund.charge, Charge.user)
    if projects:
        projects_ = Charge.allocation.has(Allocation.project.has(
            Project.id.in_(project.id for project in projects)))
        charges_q = charges_q.filter(projects_)
        refunds_q = refunds_q.filter(projects_)
    if resources:
        resources_ = Charge.allocation.has(Allocation.resource.has(
            Resource.id.in_(resource.id for resource in resources)))
        charges_q = charges_q.filter(resources_)
        refunds_q = refunds_q.filter(resources_)
    if after:
        after_ = Charge.datetime >= after
        charges_q = charges_q.filter(after_)
        refunds_q = refunds_q.filter(after_)
    if before:
        before_ = Charge.datetime < before
        charges_q = charges_q.filter(before_)
        refunds_q = refunds_q.filter(before_)
    charges_q = charges_q.subquery()
    refunds_q = refunds_q.subquery()
    query = s.query(
        User,
        func.coalesce(charges_q.c.charge_count, 0),
        cast(func.coalesce(charges_q.c.charge_sum, 0)
            - func.coalesce(refunds_q.c.refund_sum, 0), Integer)
        ).group_by(User.id)
    query = query.outerjoin(
        (charges_q, User.id == charges_q.c.user_id),
        (refunds_q, User.id == refunds_q.c.user_id)).group_by(User.id)
    query = query.order_by(User.id)
    query = query.filter(User.id.in_(user.id for user in users))
    
    charge_count_total = 0
    charge_sum_total = 0
    for user, charge_count, charge_sum in query:
        charge_count_total += charge_count
        charge_sum_total += charge_sum
        print format({'Name':user.name, 'Charges':charge_count,
            'Charged':display_units(charge_sum)})
    print format.bar(["Charges", "Charged"])
    print format({'Charges':charge_count_total,
        'Charged':display_units(charge_sum_total)})
    print unit_definition()


def print_projects_report (projects, users=None, resources=None,
                           before=None, after=None):
    
    """Projects report.
    
    The projects report lists allocations and charges for each project
    in the system.
    """
    
    format = Formatter([
        "Name", "Charges", "Charged", "Available"])
    format.widths = {'Name':15, 'Charges':7, 'Charged':15, 'Available':15}
    format.aligns = {'Charges':"right",
        'Charged':"right", "Available":"right"}
    print format.header()
    print format.bar()

    s = Session()
    allocations_q = s.query(
        Project.id.label("project_id"),
        func.sum(Allocation.amount).label("allocation_sum")
        ).group_by(Project.id)
    allocations_q = allocations_q.join(Allocation.project)
    now = datetime.now()
    allocations_q = allocations_q.filter(
        and_(Allocation.start<=now, Allocation.expiration>now))
    holds_q = s.query(
        Project.id.label("project_id"),
        func.sum(Hold.amount).label("hold_sum")).group_by(Project.id)
    holds_q = holds_q.join(Hold.allocation, Allocation.project)
    holds_q = holds_q.filter(Hold.active == True)
    charges_q = s.query(
        Project.id.label("project_id"),
        func.count(Charge.id).label("charge_count"),
        func.sum(Charge.amount).label("charge_sum")).group_by(Project.id)
    charges_q = charges_q.join(Charge.allocation, Allocation.project)
    refunds_q = s.query(
        Project.id.label("project_id"),
        func.sum(Refund.amount).label("refund_sum")).group_by(Project.id)
    refunds_q = refunds_q.join(
        Refund.charge, Charge.allocation, Allocation.project)
    if resources:
        resources_ = Allocation.resource.has(Resource.id.in_(
            resource.id for resource in resources))
        allocations_q = allocations_q.filter(resources_)
        charges_q = charges_q.filter(resources_)
        refunds_q = refunds_q.filter(resources_)
    spec_charges_q = charges_q
    spec_refunds_q = refunds_q
    if users:
        users_ = Charge.user.has(User.id.in_(user.id for user in users))
        spec_charges_q = spec_charges_q.filter(users_)
        spec_refunds_q = spec_refunds_q.filter(users_)
    if after:
        after_ = Charge.datetime >= after
        spec_charges_q = spec_charges_q.filter(after_)
        spec_refunds_q = spec_refunds_q.filter(after_)
    if before:
        before_ = Charge.datetime < before
        spec_charges_q = spec_charges_q.filter(before_)
        spec_refunds_q = spec_refunds_q.filter(before_)
    allocations_q = allocations_q.subquery()
    holds_q = holds_q.subquery()
    charges_q = charges_q.subquery()
    refunds_q = refunds_q.subquery()
    spec_charges_q = spec_charges_q.subquery()
    spec_refunds_q = spec_refunds_q.subquery()
    query = s.query(
        Project,
        func.coalesce(spec_charges_q.c.charge_count, 0),
        cast(func.coalesce(spec_charges_q.c.charge_sum, 0)
            - func.coalesce(spec_refunds_q.c.refund_sum, 0), Integer),
        cast(func.coalesce(allocations_q.c.allocation_sum, 0)
            - func.coalesce(holds_q.c.hold_sum, 0)
            - func.coalesce(charges_q.c.charge_sum, 0)
            + func.coalesce(refunds_q.c.refund_sum, 0), Integer))
    query = query.outerjoin(
        (spec_charges_q, Project.id == spec_charges_q.c.project_id),
        (spec_refunds_q, Project.id == spec_refunds_q.c.project_id),
        (allocations_q, Project.id == allocations_q.c.project_id),
        (holds_q, Project.id == holds_q.c.project_id),
        (charges_q, Project.id == charges_q.c.project_id),
        (refunds_q, Project.id == refunds_q.c.project_id))
    query = query.order_by(Project.id)
    query = query.filter(Project.id.in_(project.id for project in projects))
    
    allocation_sum_total = 0
    charge_count_total = 0
    charge_sum_total = 0
    for project, charge_count, charge_sum, allocation_sum in query:
        charge_count_total += charge_count
        charge_sum_total += charge_sum
        allocation_sum_total += allocation_sum
        print format({'Name':project.name,
            'Charges':charge_count,
            'Charged':display_units(charge_sum),
            'Available':display_units(allocation_sum)})
    print format.bar(["Charges", "Charged", "Available"])
    print format({'Charges':charge_count_total,
        'Charged':display_units(charge_sum_total),
        'Available':display_units(allocation_sum_total)})
    print unit_definition()


def print_allocations_report (allocations, users=None,
                              before=None, after=None, comments=False):
    
    """Allocations report.
    
    The allocations report lists attributes of and charges against allocations
    in the system.
    """
    
    fields = ["Allocation", "Expiration", "Resource", "Project", "Charges",
        "Charged", "Available"]
    if comments:
        fields.append("Comment")
    format = Formatter(fields)
    format.headers = {'Allocation':"#"}
    format.widths = {'Allocation':4, 'Project':15, 'Available':13,
        'Charges':7, 'Charged':13, 'Expiration':10}
    format.aligns = {'Available':"right", 'Charges':"right", 'Charged':"right"}
    print format.header()
    print format.bar()
    
    s = Session()
    holds_q = s.query(
        Allocation.id.label("allocation_id"),
        func.sum(Hold.amount).label("hold_sum")).group_by(Allocation.id)
    holds_q = holds_q.join(Hold.allocation)
    holds_q = holds_q.filter(Hold.active == True)
    charges_q = s.query(
        Allocation.id.label("allocation_id"),
        func.count(Charge.id).label("charge_count"),
        func.sum(Charge.amount).label("charge_sum")).group_by(Allocation.id)
    charges_q = charges_q.join(Charge.allocation)
    refunds_q = s.query(
        Allocation.id.label("allocation_id"),
        func.sum(Refund.amount).label("refund_sum")).group_by(Allocation.id)
    refunds_q = refunds_q.join(
        Refund.charge, Charge.allocation)
    spec_charges_q = charges_q
    spec_refunds_q = refunds_q
    if users:
        users_ = Charge.user.has(User.id.in_(user.id for user in users))
        spec_charges_q = spec_charges_q.filter(users_)
        spec_refunds_q = spec_refunds_q.filter(users_)
    if after:
        after_ = Charge.datetime >= after
        spec_charges_q = spec_charges_q.filter(after_)
        spec_refunds_q = spec_refunds_q.filter(after_)
    if before:
        before_ = Charge.datetime < before
        spec_charges_q = spec_charges_q.filter(before_)
        spec_refunds_q = spec_refunds_q.filter(before_)
    holds_q = holds_q.subquery()
    charges_q = charges_q.subquery()
    refunds_q = refunds_q.subquery()
    spec_charges_q = spec_charges_q.subquery()
    spec_refunds_q = spec_refunds_q.subquery()
    query = s.query(
        Allocation,
        func.coalesce(spec_charges_q.c.charge_count, 0),
        cast(func.coalesce(spec_charges_q.c.charge_sum, 0)
            - func.coalesce(spec_refunds_q.c.refund_sum, 0), Integer),
        cast(Allocation.amount
            - func.coalesce(holds_q.c.hold_sum, 0)
            - func.coalesce(charges_q.c.charge_sum, 0)
            + func.coalesce(refunds_q.c.refund_sum, 0), Integer))
    query = query.outerjoin(
        (spec_charges_q, Allocation.id == spec_charges_q.c.allocation_id),
        (spec_refunds_q, Allocation.id == spec_refunds_q.c.allocation_id),
        (holds_q, Allocation.id == holds_q.c.allocation_id),
        (charges_q, Allocation.id == charges_q.c.allocation_id),
        (refunds_q, Allocation.id == refunds_q.c.allocation_id))
    query = query.order_by(Allocation.id)
    query = query.filter(Allocation.id.in_(
        allocation.id for allocation in allocations))
    
    charge_count_total = 0
    charge_sum_total = 0
    allocation_sum_total = 0
    for allocation, charge_count, charge_sum, allocation_sum in query:
        charge_count_total += charge_count
        charge_sum_total += charge_sum
        allocation_sum_total += allocation_sum
        print format({
            'Allocation':allocation.id,
            'Project':allocation.project,
            'Resource':allocation.resource,
            'Expiration':format_datetime(allocation.expiration),
            'Charges':charge_count,
            'Charged':display_units(charge_sum),
            'Available':display_units(allocation_sum),
            'Comment':allocation.comment})
    print format.bar(["Available", "Charges", "Charged"])
    print format({
        'Charges':charge_count_total,
        'Charged':display_units(charge_sum_total),
        'Available':display_units(allocation_sum_total)})
    print unit_definition()


def print_holds_report (holds, comments=None):
    
    """Holds report.
    
    The holds report displays individual holds.
    """
    fields = ["Hold", "Datetime", "Resource", "Project", "User", "Held"]
    if comments:
        fields.allend("Comment")
    format = Formatter(fields)
    format.headers = {'Hold':"#", 'Datetime':"Date"}
    format.widths = {
        'Hold':6, 'User':8, 'Project':15, 'Held':13, 'Datetime':10}
    format.aligns = {'Held':"right"}
    print format.header()
    print format.bar()
    
    query = Session().query(Hold)
    query = query.options(eagerload(
        Hold.user, Hold.allocation, Allocation.project, Allocation.resource))
    query = query.filter(Hold.id.in_(hold.id for hold in holds))
    query = query.order_by(Hold.datetime, Hold.id)
    
    hold_sum = 0
    for hold in holds:
        hold_sum += hold.amount
        print format({
            'Hold':hold.id,
            'User':hold.user,
            'Project':hold.allocation.project,
            'Resource':hold.allocation.resource,
            'Datetime':format_datetime(hold.datetime),
            'Held':display_units(hold.amount),
            'Comment':hold.comment})
    print format.bar(["Held"])
    print format({'Held':display_units(hold_sum)})
    print unit_definition()


def print_charges_report (charges, comments=False):
    
    """Charges report.
    
    The charges report displays individual charges.
    """
    fields = ["Charge", "Datetime", "Resource", "Project", "User", "Charged"]
    if comments:
        fields.append("Comment")
    format = Formatter(fields)
    format.headers = {'Charge':"#", 'Datetime':"Date"}
    format.widths = {
        'Charge':6, 'User':8, 'Project':15, 'Charged':13, 'Datetime':10}
    format.aligns = {'Charged':"right"}
    print format.header()
    print format.bar()
    
    s = Session()
    query = s.query(Charge,
        cast(Charge.amount
            - func.coalesce(func.sum(Refund.amount), 0), Integer)
        ).group_by(Charge.id)
    query = query.outerjoin(Charge.refunds)
    query = query.options(eagerload(Charge.user, Charge.allocation,
        Allocation.project, Allocation.resource))
    query = query.order_by(Charge.datetime, Charge.id)
    query = query.filter(Charge.id.in_(charge.id for charge in charges))
    
    total_charged = 0
    for charge, charge_amount in query:
        total_charged += charge_amount
        print format({
            'Charge':charge,
            'User':charge.user,
            'Project':charge.allocation.project,
            'Resource':charge.allocation.resource,
            'Datetime':format_datetime(charge.datetime),
            'Charged':display_units(charge_amount),
            'Comment':charge.comment})
    print format.bar(["Charged"])
    print format({'Charged':display_units(total_charged)})
    print unit_definition()


def print_allocations (allocations):
    for allocation in allocations:
        print_allocation(allocation)


def print_allocation (allocation):
    amount = display_units(allocation.amount)
    allocation_str = "Allocation %s -- %s" % (allocation, amount)
    print allocation_str
    print " * Datetime: %s" % allocation.datetime
    print " * Project: %s" % allocation.project
    print " * Resource: %s" % allocation.resource
    print " * Start: %s" % allocation.start
    print " * Expiration: %s" % allocation.expiration
    print " * Comment: %s" % allocation.comment


def print_holds (holds):
    for hold in holds:
        print_hold(hold)


def print_hold (hold):
    print "Hold %s -- %s" % (hold, display_units(hold.amount))
    print " * Datetime: %s" % hold.datetime
    print " * Active: %s" % hold.active
    print " * Allocation: %s" % hold.allocation
    print " * Project: %s" % hold.allocation.project
    print " * Resource: %s" % hold.allocation.resource
    print " * Comment: %s" % hold.comment


def print_charges (charges):
    for charge in charges:
        print_charge(charge)


def print_charge (charge):
    charge_str = "Charge %s -- %s" % (
        charge, display_units(charge.effective_amount))
    if charge.amount != charge.effective_amount:
        addendum = "(originally %s)" % display_units(charge.amount)
        charge_str = " ".join([charge_str, addendum])
    print charge_str
    print " * Datetime: %s" % charge.datetime
    print " * Allocation: %s" % charge.allocation
    print " * Project: %s" % charge.allocation.project
    print " * Resource: %s" % charge.allocation.resource
    print " * Comment: %s" % charge.comment


def print_refunds (refunds):
    for refund in refunds:
        print_refund(refund)


def print_refund (refund):
    print "Refund %s -- %s" % (refund, display_units(refund.amount))
    print " * Datetime: %s" % refund.datetime
    print " * Charge: %s" % refund.charge
    print " * Allocation: %s" % refund.charge.allocation
    print " * Project: %s" % refund.charge.allocation.project
    print " * Resource: %s" % refund.charge.allocation.resource
    print " * Comment: %s" % refund.comment


def unit_definition ():
    try:
        unit_label = config.get("cbank", "unit_label")
    except ConfigParser.Error:
        return "Units are undefined."
    else:
        return "Units are in %s." % unit_label

def display_units (amount):
    converted_amount = convert_units(amount)
    if 0 < converted_amount < 0.1:
        return "< 0.1"
    else:
        return locale.format("%.1f", converted_amount, True)

def convert_units (amount):
    mul, div = get_unit_factor()
    return amount * mul / div

def format_datetime (dt):
    return dt.strftime("%Y-%m-%d")


class Formatter (object):
    
    def __init__ (self, fields, **kwargs):
        self.fields = fields
        self.headers = kwargs.get("headers", {})
        self.widths = kwargs.get("widths", {})
        self.aligns = kwargs.get("aligns", {})
    
    def __call__ (self, *args, **kwargs):
        return self.format(*args, **kwargs)
    
    def format (self, data):
        formatted_data = []
        for field in self.fields:
            datum = data.get(field, "")
            datum = str(datum)
            align = self._get_align(field)
            width = self._get_width(field)
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
    
    def _get_align (self, field):
        return self.aligns.get(field, "left")
    
    def _get_width (self, field):
        return self.widths.get(field, len(self._get_header(field)))
    
    def _get_header (self, field):
        return self.headers.get(field, field)
    
    def bar (self, fields=None):
        if fields is None:
            fields = self.fields
        bars = {}
        for field in fields:
            bars[field] = "-" * (self._get_width(field))
        return self.format(bars)
    
    def header (self, fields=None):
        if fields is None:
            fields = self.fields
        headers = {}
        for field in fields:
            headers[field] = self._get_header(field)
        return self.format(headers)

