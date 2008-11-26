"""Views of the model provided by cbank.

Classes:
Formatter -- a tabular formatter for reports

Functions:
print_users_report -- charges for users
print_projects_report -- allocations and charges for projects
print_allocations_report -- allocation and charges for allocations
print_holds_report -- a table of holds
print_jobs_report -- a table of jobs
print_charges_report -- a table of charges
print_allocations -- print multiple allocations
print_allocation -- print a single allocation
print_holds -- print multiple holds
print_hold -- print a single hold
print_jobs -- print multiple jobs
print_job -- print a single job
print_charges -- print multiple charges
print_charge -- print a single charge
print_refunds -- print multiple refunds
print_refund -- print a single refund
unit_definition -- a configured unit description
display_units -- print units in a locale-specific format
convert_units -- convert an amount to the specified units
format_datetime -- convert a datetime to a string
"""

import sys
import locale
import ConfigParser
from datetime import datetime, timedelta

from sqlalchemy.sql import and_, cast, func
from sqlalchemy.types import Integer
from sqlalchemy.orm import eagerload

from clusterbank import config
from clusterbank.cbank.common import get_unit_factor
from clusterbank.model import (User, Project, Resource, Allocation, Hold,
    Job, Charge, Refund)
from clusterbank.controllers import Session

__all__ = ["unit_definition", "convert_units", "display_units",
    "print_users_report", "print_projects_report", "print_allocations_report",
    "print_holds_report", "print_jobs_report", "print_charges_report"]


locale.setlocale(locale.LC_ALL, locale.getdefaultlocale()[0])


def print_users_report (users, projects=None, resources=None,
                        after=None, before=None):
    
    """Users report.
    
    The users report lists the number of charges and total amount charged
    for each specified user.
    
    Arguments:
    users -- users to report
    
    Keyword arguments:
    projects -- only show charges for these projects
    resources -- only show charges for these resources
    after -- only show charges after this datetime (inclusive)
    before -- only show charges before this datetime (exclusive)
    """
    
    format = Formatter(["Name", "Charges", "Charged"])
    format.widths = {'Name':10, 'Charges':8, 'Charged':15}
    format.truncate = {'Name':True}
    format.aligns = {'Charges':"right", 'Charged':"right"}
    print >> sys.stderr, format.header()
    print >> sys.stderr, format.separator()
    
    s = Session()
    charges_q = s.query(
        User.id.label("user_id"),
        func.count(Charge.id).label("charge_count"),
        func.sum(Charge.amount).label("charge_sum")).group_by(User.id)
    charges_q = charges_q.join(Charge.jobs, Job.user)
    refunds_q = s.query(
        User.id.label("user_id"),
        func.sum(Refund.amount).label("refund_sum")).group_by(User.id)
    refunds_q = refunds_q.join(Refund.charge, Charge.jobs, Job.user)
    
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
    query = query.filter(User.id.in_(user.id for user in users))
    query = query.order_by(User.id)
    
    charge_count_total = 0
    charge_sum_total = 0
    for user, charge_count, charge_sum in query:
        charge_count_total += charge_count
        charge_sum_total += charge_sum
        print format({'Name':user.name, 'Charges':charge_count,
            'Charged':display_units(charge_sum)})
    print >> sys.stderr, format.separator(["Charges", "Charged"])
    print >> sys.stderr, format({'Charges':charge_count_total,
        'Charged':display_units(charge_sum_total)})
    print >> sys.stderr, unit_definition()


def print_projects_report (projects, users=None, resources=None,
                           before=None, after=None):
    
    """Projects report.
    
    The projects report lists allocations and charges for each project
    in the system.
    
    Arguments:
    projects -- projects to report
    
    Keyword arguments:
    users -- only show charges by these users
    resources -- only show charges for these resources
    after -- only show charges after this datetime (inclusive)
    before -- only show charges before this datetime (exclusive)
    """
    
    format = Formatter([
        "Name", "Charges", "Charged", "Available"])
    format.widths = {'Name':15, 'Charges':7, 'Charged':15, 'Available':15}
    format.truncate = {'Name':True}
    format.aligns = {'Charges':"right",
        'Charged':"right", "Available":"right"}
    print >> sys.stderr, format.header()
    print >> sys.stderr, format.separator()
    
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
        users_ = Charge.jobs.any(Job.user.has(User.id.in_(
            user.id for user in users)))
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
    print >> sys.stderr, format.separator(["Charges", "Charged", "Available"])
    print >> sys.stderr, format({'Charges':charge_count_total,
        'Charged':display_units(charge_sum_total),
        'Available':display_units(allocation_sum_total)})
    print >> sys.stderr, unit_definition()


def print_allocations_report (allocations, users=None,
                              before=None, after=None, comments=False):
    
    """Allocations report.
    
    The allocations report lists attributes of and charges against allocations
    in the system.
    
    Arguments:
    allocations -- allocations to report
    
    Keyword arguments:
    users -- only show charges by these users
    after -- only show charges after this datetime (inclusive)
    before -- only show charges before this datetime (exclusive)
    comments -- report allocation comments
    """
    
    fields = ["Allocation", "Expiration", "Resource", "Project", "Charges",
        "Charged", "Available"]
    if comments:
        fields.append("Comment")
    format = Formatter(fields)
    format.headers = {'Allocation':"#"}
    format.widths = {'Allocation':4, 'Project':15, 'Available':13,
        'Charges':7, 'Charged':13, 'Expiration':10}
    format.truncate = {'Resource':True, 'Project':True}
    format.aligns = {'Available':"right", 'Charges':"right", 'Charged':"right"}
    print >> sys.stderr, format.header()
    print >> sys.stderr, format.separator()
    
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
        users_ = Charge.jobs.any(Job.user.has(User.id.in_(
            user.id for user in users)))
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
    query = query.filter(Allocation.id.in_(
        allocation.id for allocation in allocations))
    query = query.order_by(Allocation.id)
    
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
    print >> sys.stderr, format.separator(["Available", "Charges", "Charged"])
    print >> sys.stderr, format({
        'Charges':charge_count_total,
        'Charged':display_units(charge_sum_total),
        'Available':display_units(allocation_sum_total)})
    print >> sys.stderr, unit_definition()


def print_holds_report (holds, comments=None):
    
    """Holds report.
    
    The holds report displays individual holds.
    
    Arguments:
    holds -- holds to report
    
    Keyword arguments:
    comments -- report hold comments
    """
    
    fields = ["Hold", "Date", "Resource", "Project", "User", "Held"]
    if comments:
        fields.append("Comment")
    format = Formatter(fields)
    format.headers = {'Hold':"#"}
    format.widths = {
        'Hold':6, 'User':8, 'Project':15, 'Held':13, 'Date':10}
    format.truncate = {'User':True, 'Project':True, 'Resource':True}
    format.aligns = {'Held':"right"}
    print >> sys.stderr, format.header()
    print >> sys.stderr, format.separator()
    
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
            'Date':format_datetime(hold.datetime),
            'Held':display_units(hold.amount),
            'Comment':hold.comment})
    print >> sys.stderr, format.separator(["Held"])
    print >> sys.stderr, format({'Held':display_units(hold_sum)})
    print >> sys.stderr, unit_definition()


def print_jobs_report (jobs):
    
    """Jobs report.
    
    The jobs report displays individual jobs.
    
    Arguments:
    jobs -- jobs to report
    """
    
    format = Formatter(["ID", "Name", "User", "Account", "Duration",
        "Charged"])
    format.aligns = {'Duration':"right", 'Charged':"right"}
    format.widths = {'ID':19, 'Name': 10, 'User':8, 'Account':15,
        'Duration':9, 'Charged':13}
    format.truncate = {'Name':True, 'User':True, 'Account':True}
    print >> sys.stderr, format.header()
    print >> sys.stderr, format.separator()
    duration_sum = timedelta()
    charge_sum = 0
    for job in jobs:
        try:
            duration_td = job.end - job.start
        except TypeError:
            duration = None
        else:
            duration_sum += duration_td
            duration = format_timedelta(duration_td)
        charged = sum(charge.effective_amount() for charge in job.charges)
        charge_sum += charged
        if job.start is not None:
            start = format_datetime(job.start)
        else:
            start = ""
        print format({
            'ID':job.id,
            'Name':job.name or "",
            'User':job.user or "",
            'Account':job.account or "",
            'Duration':duration or "",
            'Charged':display_units(charged)})
    print >> sys.stderr, format.separator(["Duration", "Charged"])
    print >> sys.stderr, format({'Duration':format_timedelta(duration_sum),
        'Charged':display_units(charge_sum)})
    print >> sys.stderr, unit_definition()


def format_timedelta (td):
    hours = (td.days * 24) + (td.seconds // (60 * 60))
    minutes = (td.seconds % (60 * 60)) / 60
    seconds = td.seconds % 60
    return "%i:%.2i:%.2i" % (hours, minutes, seconds)


def print_charges_report (charges, comments=False):
    
    """Charges report.
    
    The charges report displays individual charges.
    
    Arguments:
    charges -- charges to report
    
    Keyword arguments:
    comments -- report charge comments
    """
    
    fields = ["Charge", "Date", "Resource", "Project", "Charged"]
    if comments:
        fields.append("Comment")
    format = Formatter(fields)
    format.headers = {'Charge':"#"}
    format.widths = {
        'Charge':6, 'Project':15, 'Charged':13, 'Date':10}
    format.truncate = {'Resource':True, 'Project':True}
    format.aligns = {'Charged':"right"}
    print >> sys.stderr, format.header()
    print >> sys.stderr, format.separator()
    
    s = Session()
    query = s.query(Charge,
        cast(Charge.amount
            - func.coalesce(func.sum(Refund.amount), 0), Integer)
        ).group_by(Charge.id)
    query = query.outerjoin(Charge.refunds)
    query = query.options(eagerload(Charge.allocation,
        Allocation.project, Allocation.resource))
    query = query.filter(Charge.id.in_(charge.id for charge in charges))
    query = query.order_by(Charge.datetime, Charge.id)
    
    total_charged = 0
    for charge, charge_amount in query:
        total_charged += charge_amount
        print format({
            'Charge':charge.id,
            'Project':charge.allocation.project,
            'Resource':charge.allocation.resource,
            'Date':format_datetime(charge.datetime),
            'Charged':display_units(charge_amount),
            'Comment':charge.comment})
    print >> sys.stderr, format.separator(["Charged"])
    print >> sys.stderr, format({'Charged':display_units(total_charged)})
    print >> sys.stderr, unit_definition()


def print_allocations (allocations):
    """Print multiple allocations with print_allocation."""
    for allocation in allocations:
        print_allocation(allocation)


def print_allocation (allocation):
    """Print an allocation in user-friendly detail."""
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
    """Print multiple holds with print_hold."""
    for hold in holds:
        print_hold(hold)


def print_hold (hold):
    """Print a hold in user-friendly detail."""
    print "Hold %s -- %s" % (hold, display_units(hold.amount))
    print " * Datetime: %s" % hold.datetime
    print " * Active: %s" % hold.active
    print " * Allocation: %s" % hold.allocation
    print " * Project: %s" % hold.allocation.project
    print " * Resource: %s" % hold.allocation.resource
    print " * Comment: %s" % hold.comment


def print_jobs (jobs):
    """Print multiple jobs with print_job."""
    for job in jobs:
        print_job(job)


def print_job (job):
    print "Job %s" % job
    print " * User: %s" % job.user
    print " * Group: %s" % job.group
    print " * Account: %s" % job.account
    print " * Name: %s" % job.name
    print " * Queue: %s" % job.queue
    print " * Reservation name: %s" % job.reservation_name
    print " * Reservation id: %s" % job.reservation_id
    print " * Creation time: %s" % job.ctime
    print " * Queue time: %s" % job.qtime
    print " * Eligible time: %s" % job.etime
    print " * Start: %s" % job.start
    print " * Execution host: %s" % job.exec_host
    print " * Resource list:"
    for key, value in job.resource_list.iteritems():
        print "    * %s: %s" % (key, value)
    print " * Session: %s" % job.session
    print " * Alternate id: %s" % job.alternate_id
    print " * End: %s" % job.end
    print " * Exit status: %s" % job.exit_status
    print " * Resources used:"
    for key, value in job.resources_used.iteritems():
        print "    * %s: %s" % (key, value)
    print " * Accounting id: %s" % job.accounting_id


def print_charges (charges):
    """Print multiple charges with print_charge."""
    for charge in charges:
        print_charge(charge)


def print_charge (charge):
    """Print a charge in user-friendly detail."""
    charge_str = "Charge %s -- %s" % (
        charge, display_units(charge.effective_amount()))
    if charge.amount != charge.effective_amount():
        addendum = "(originally %s)" % display_units(charge.amount)
        charge_str = " ".join([charge_str, addendum])
    print charge_str
    print " * Datetime: %s" % charge.datetime
    print " * Allocation: %s" % charge.allocation
    print " * Project: %s" % charge.allocation.project
    print " * Resource: %s" % charge.allocation.resource
    print " * Comment: %s" % charge.comment


def print_refunds (refunds):
    """Print multiple refunds with print_refund."""
    for refund in refunds:
        print_refund(refund)


def print_refund (refund):
    """Print a refund in user-friendly detail."""
    print "Refund %s -- %s" % (refund, display_units(refund.amount))
    print " * Datetime: %s" % refund.datetime
    print " * Charge: %s" % refund.charge
    print " * Allocation: %s" % refund.charge.allocation
    print " * Project: %s" % refund.charge.allocation.project
    print " * Resource: %s" % refund.charge.allocation.resource
    print " * Comment: %s" % refund.comment


def unit_definition ():
    """A configured unit description."""
    try:
        unit_label = config.get("cbank", "unit_label")
    except ConfigParser.Error:
        return "Units are undefined."
    else:
        return "Units are in %s." % unit_label


def display_units (amount):
    """A locale-specific view of units."""
    converted_amount = convert_units(amount)
    if 0 < converted_amount < 0.1:
        return "< 0.1"
    else:
        return locale.format("%.1f", converted_amount, True)


def convert_units (amount):
    """Convert an amount to the configured units."""
    mul, div = get_unit_factor()
    return amount * mul / div


def format_datetime (datetime_):
    """Convert a datetime to a string using a default format."""
    return datetime_.strftime("%Y-%m-%d")


class Formatter (object):
    """A tabular formatter for reports.
    
    Attributes:
    fields -- the fields (columns) in the table
    headers -- a field-->header dict
    widths -- a field-->column width dict
    aligns -- a field-->alignment (left, right, center) dict
    
    Methods:
    format -- format a dictionary as a line in the table
    separator -- a separator line
    header -- a header line
    """
    
    def __init__ (self, fields):
        """Initialize a new formatter from a list of fields."""
        self.fields = fields
        self.headers = {}
        self.widths = {}
        self.aligns = {}
        self.truncate = {}
    
    def __call__ (self, *args, **kwargs):
        """Convenience access to format()."""
        return self.format(*args, **kwargs)
    
    def format (self, data):
        """Format a dict as a line in a table."""
        formatted_data = []
        for field in self.fields:
            datum = data.get(field, "")
            datum = str(datum)
            align = self._get_align(field)
            width = self._get_width(field)
            if self._get_truncate(field):
                datum = datum[:width]
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
    
    def _get_truncate (self, field):
        return self.truncate.get(field, False)
    
    def _get_header (self, field):
        return self.headers.get(field, field)
    
    def separator (self, fields=None):
        """A separator line.
        
        Arguments:
        fields -- a list of the fields to include a separator for
        
        Notes:
        By default, fields includes all defined fields.
        """
        if fields is None:
            fields = self.fields
        separators = {}
        for field in fields:
            separators[field] = "-" * (self._get_width(field))
        return self.format(separators)
    
    def header (self, fields=None):
        """A header line.
        
        Arguments:
        fields -- a list of the fields to include a header for
        
        Notes:
        By default, fields, includes all defined fields.
        """
        if fields is None:
            fields = self.fields
        headers = {}
        for field in fields:
            headers[field] = self._get_header(field)
        return self.format(headers)

