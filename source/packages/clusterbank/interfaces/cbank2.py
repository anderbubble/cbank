"""cbank part deux"""

import sys
import os
import pwd
from itertools import izip
import string

from clusterbank.model import User, Project, Allocation, Charge, Refund

reports_available = ["allocations", "charges"]

class CbankException (Exception): pass

class CbankError (CbankException): pass

class UnknownUser (CbankError): pass

class UnknownReport (CbankError):
    
    def __str__ (self):
        return "unknown report: %s" % CbankError.__str__(self)

def main ():
    handle_exceptions(run)

def handle_exceptions (func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except KeyboardInterrupt:
        sys.exit(1)
    except CbankError, e:
        print >> sys.stderr, "cbank:", e
        sys.exit(1)

def run ():
    report_type = get_report_type()
    if report_type == "allocations":
        allocations = get_allocations()
        display_allocations(allocations)
    elif report_type == "charges":
        charges = get_charges()
        display_charges(charges)
    else:
        raise UnknownReport(report_type)

def get_report_type ():
    try:
        requested_report = sys.argv[1]
    except IndexError:
        return "allocations" # default report type
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

def get_allocations ():
    user = get_current_user()
    project_ids = [project.id for project in user.projects]
    allocations = Allocation.query.filter(Allocation.project.has(Project.id.in_(project_ids)))
    return allocations

def get_charges ():
    user = get_current_user()
    project_ids = [project.id for project in user.projects]
    charges = Charge.query.filter_by(user=user).filter(Charge.allocation.has(Allocation.project.has(Project.id.in_(project_ids))))
    return charges

def display_allocations (allocations):
    format = Formatter([10, 15, 15, (10, string.rjust), (10, string.rjust)])
    print >> sys.stderr, format(["Expires", "Resource", "Project", "Total", "Available"])
    print >> sys.stderr, format(["-"*10, "-"*15, "-"*15, "-"*10, "-"*10])
    for allocation in allocations:
        print format([allocation.expiration.strftime("%Y-%m-%d"), allocation.resource, allocation.project, allocation.amount, allocation.amount_available])

def display_charges (charges):
    format = Formatter([10, 15, 15, (10, string.rjust)])
    print >> sys.stderr, format(["Date", "Resource", "Project", "Amount"])
    print >> sys.stderr, format(["-"*10, "-"*15, "-"*15, "-"*10])
    for charge in charges:
        print format([charge.datetime.strftime("%Y-%m-%d"), charge.allocation.resource, charge.allocation.project, charge.effective_amount])
    print >> sys.stderr, format(["", "", "", "-"*10])
    total = int(charges.sum(Charge.amount) or 0) - int(charges.join("refunds").sum(Refund.amount) or 0)
    print >> sys.stderr, format(["", "", "", total]), "(total)"

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
    user = User.by_name(username)
    if not user:
        raise UnknownUser("User '%s' was not found." % username)
    return user

