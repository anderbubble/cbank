"""Views of the model provided by the cli."""

import sys
import locale
import ConfigParser
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy.sql import and_, func
from sqlalchemy.orm import eagerload

from cbank import config
from cbank.cli.common import get_unit_factor
from cbank.model import (
    Session, User, Project, Resource,
    Allocation, Hold, Job, Charge, Refund)
import cbank.model.queries

__all__ = [
    "unit_definition", "convert_units", "display_units",
    "print_users_list", "print_projects_list", "print_allocations_list",
    "print_holds_list", "print_jobs_list", "print_charges_list"]


locale.setlocale(locale.LC_ALL, locale.getdefaultlocale()[0])


def print_users_list (users, truncate=True, **kwargs):
    
    """Users list.
    
    The users list lists the number of charges and total amount charged
    for each specified user.
    """
    
    format = Formatter(["Name", "Jobs", "Charged"])
    format.widths = {'Name':10, 'Jobs':8, 'Charged':15}
    if truncate:
        format.truncate = {'Name':True}
    format.aligns = {'Jobs':"right", 'Charged':"right"}
    print >> sys.stderr, format.header()
    print >> sys.stderr, format.separator()
    job_count_total = 0
    charge_sum_total = 0
    users_printed = []
    if users:
        data = cbank.model.queries.user_summary(users, **kwargs)
        for user_id, job_count, charge_sum in data:
            user = User.cached(user_id)
            users_printed.append(user)
            job_count_total += job_count
            charge_sum_total += charge_sum
            print format({'Name':user, 'Jobs':job_count,
                'Charged':display_units(charge_sum)})
        for user in users:
            if user not in users_printed:
                print format({'Name':user, 'Jobs':0,
                          'Charged':display_units(0)})
    print >> sys.stderr, format.separator(["Jobs", "Charged"])
    print >> sys.stderr, format({'Jobs':job_count_total,
        'Charged':display_units(charge_sum_total)})
    print >> sys.stderr, unit_definition()


def print_projects_list (projects, truncate=True, **kwargs):
    
    """Projects list.
    
    The projects list lists allocations and charges for each project
    in the system.
    """
    
    format = Formatter([
        "Name", "Jobs", "Charged", "Available"])
    format.widths = {'Name':15, 'Jobs':7, 'Charged':15, 'Available':15}
    if truncate:
        format.truncate = {'Name':True}
    format.aligns = {'Jobs':"right",
        'Charged':"right", "Available":"right"}
    print >> sys.stderr, format.header()
    print >> sys.stderr, format.separator()

    allocation_sum_total = 0
    job_count_total = 0
    charge_sum_total = 0
    if projects:
        projects_displayed = []
        data = cbank.model.queries.project_summary(projects, **kwargs)
        for project_id, job_count, charge_sum, allocation_sum in data:
            project = Project.cached(project_id)
            projects_displayed.append(project)
            job_count_total += job_count
            charge_sum_total += charge_sum
            allocation_sum_total += allocation_sum
            print format({
                'Name':project,
                'Jobs':job_count,
                'Charged':display_units(charge_sum),
                'Available':display_units(allocation_sum)})
        for project in projects:
            if project not in projects_displayed:
                print format({
                    'Name':project,
                    'Jobs':0,
                    'Charged':display_units(0),
                    'Available':display_units(0)})
    print >> sys.stderr, format.separator([
        "Jobs", "Charged", "Available"])
    print >> sys.stderr, format({
        'Jobs':job_count_total,
        'Charged':display_units(charge_sum_total),
        'Available':display_units(allocation_sum_total)})
    print >> sys.stderr, unit_definition()


def print_allocations_list (allocations, truncate=True, comments=False, **kwargs):
    
    """Allocations list.
    
    The allocations list lists attributes of and charges against allocations
    in the system.
    
    Arguments:
    allocations -- allocations to list
    
    Keyword arguments:
    users -- only show charges by these users
    after -- only show charges after this datetime (inclusive)
    before -- only show charges before this datetime (exclusive)
    comments -- list allocation comments
    """

    fields = ["Allocation", "End", "Resource", "Project", "Jobs",
        "Charged", "Available"]
    if comments:
        fields.append("Comment")
    format = Formatter(fields)
    format.headers = {'Allocation':"#"}
    format.widths = {'Allocation':4, 'Project':15, 'Available':13,
        'Jobs':7, 'Charged':13, 'End':10}
    if truncate:
        format.truncate = {'Resource':True, 'Project':True}
    format.aligns = {'Available':"right", 'Jobs':"right", 'Charged':"right"}
    print >> sys.stderr, format.header()
    print >> sys.stderr, format.separator()

    job_count_total = 0
    charge_sum_total = 0
    allocation_sum_total = 0
    if allocations:
        data = cbank.model.queries.allocation_summary(allocations, **kwargs)
        for allocation, job_count, charge_sum, allocation_sum in data:
            job_count_total += job_count
            charge_sum_total += charge_sum
            allocation_sum_total += allocation_sum
            print format({
                'Allocation':allocation.id,
                'Project':allocation.project,
                'Resource':allocation.resource,
                'End':format_datetime(allocation.end),
                'Jobs':job_count,
                'Charged':display_units(charge_sum),
                'Available':display_units(allocation_sum),
            'Comment':(allocation.comment or "")})
    print >> sys.stderr, format.separator(["Available", "Jobs", "Charged"])
    print >> sys.stderr, format({
        'Jobs':job_count_total,
        'Charged':display_units(charge_sum_total),
        'Available':display_units(allocation_sum_total)})
    print >> sys.stderr, unit_definition()


def print_holds_list (holds, comments=False, truncate=True):
    
    """Holds list.
    
    The holds list displays individual holds.
    
    Arguments:
    holds -- holds to list
    
    Keyword arguments:
    comments -- list hold comments
    """
    
    fields = ["Hold", "Date", "Resource", "Project", "Held"]
    if comments:
        fields.append("Comment")
    format = Formatter(fields)
    format.headers = {'Hold':"#"}
    format.widths = {
        'Hold':6, 'Project':15, 'Held':13, 'Date':10}
    if truncate:
        format.truncate = {'Project':True, 'Resource':True}
    format.aligns = {'Held':"right"}
    print >> sys.stderr, format.header()
    print >> sys.stderr, format.separator()
    
    hold_sum = 0
    for hold in holds:
        hold_sum += hold.amount
        print format({
            'Hold':hold.id,
            'Project':hold.allocation.project,
            'Resource':hold.allocation.resource,
            'Date':format_datetime(hold.datetime),
            'Held':display_units(hold.amount),
            'Comment':(hold.comment or "")})
    print >> sys.stderr, format.separator(["Held"])
    print >> sys.stderr, format({'Held':display_units(hold_sum)})
    print >> sys.stderr, unit_definition()


def print_jobs_list (jobs, truncate=True):
    
    """Jobs list.
    
    The jobs list displays individual jobs.
    
    Arguments:
    jobs -- jobs to list
    """
    
    format = Formatter(["ID", "Name", "User", "Account", "Duration",
        "Charged"])
    format.aligns = {'Duration':"right", 'Charged':"right"}
    format.widths = {'ID':19, 'Name': 10, 'User':8, 'Account':15,
        'Duration':9, 'Charged':13}
    if truncate:
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


def print_charges_list (charges, comments=False, truncate=True):
    
    """Charges list.
    
    The charges list displays individual charges.
    
    Arguments:
    charges -- charges to list
    
    Keyword arguments:
    comments -- list charge comments
    """
    
    fields = ["Charge", "Date", "Resource", "Project", "Charged"]
    if comments:
        fields.append("Comment")
    format = Formatter(fields)
    format.headers = {'Charge':"#"}
    format.widths = {
        'Charge':6, 'Project':15, 'Charged':13, 'Date':10}
    if truncate:
        format.truncate = {'Resource':True, 'Project':True}
    format.aligns = {'Charged':"right"}
    print >> sys.stderr, format.header()
    print >> sys.stderr, format.separator()
    
    total_charged = 0
    for charge in charges:
        charge_amount = charge.effective_amount()
        total_charged += charge_amount
        print format({
            'Charge':charge.id,
            'Project':charge.allocation.project,
            'Resource':charge.allocation.resource,
            'Date':format_datetime(charge.datetime),
            'Charged':display_units(charge_amount),
            'Comment':(charge.comment or "")})
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
    print " * End: %s" % allocation.end
    print " * Comment: %s" % (allocation.comment or "")


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
    print " * Comment: %s" % (hold.comment or "")
    print " * Job: %s" % hold.job


def print_jobs (jobs):
    """Print multiple jobs with print_job."""
    for job in jobs:
        print_job(job)


def print_job (job):
    """Print a job in user-friendly detail."""
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
    print " * Comment: %s" % (charge.comment or "")
    print " * Job: %s" % charge.job


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
    print " * Comment: %s" % (refund.comment or "")
    print " * Job: %s" % refund.charge.job


def unit_definition ():
    """A configured unit description."""
    try:
        unit_label = config.get("cli", "unit_label")
    except ConfigParser.Error:
        return "Units are undefined."
    else:
        return "Units are in %s." % unit_label


def display_units (amount):
    """A user-friendly view of units."""
    amount_ = convert_units(amount)
    return ("%.1f" %  amount_)


def convert_units (amount):
    """Convert an amount to the configured units."""
    mul, div = get_unit_factor()
    return Decimal(str(amount)) * Decimal(str(mul)) / Decimal(str(div))


def format_datetime (datetime_):
    """Convert a datetime to a string using a default format."""
    return datetime_.strftime("%Y-%m-%d")


class Formatter (object):
    """A tabular formatter for lists.
    
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

