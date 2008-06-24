import optparse
import os
import sys
import pwd
import ConfigParser
from datetime import datetime

import clusterbank
from clusterbank.model import user_by_name
import clusterbank.cbank.exceptions as exceptions
import clusterbank.cbank.views as views

config = ConfigParser.SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

reports_available = ["use", "usage", "projects", "allocations", "charges"]

try:
    dt_strptime = datetime.strprime
except AttributeError:
    import time
    def dt_strptime (value, format):
        return datetime(*time.strptime(value, format)[0:6])

def handle_exceptions (func):
    def decorated_func (*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            sys.exit(1)
        except exceptions.CbankError, e:
            print >> sys.stderr, e
            sys.exit(1)
    decorated_func.__name__ = func.__name__
    decorated_func.__doc__ = func.__doc__
    decorated_func.__dict__.update(func.__dict__)
    return decorated_func

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

@handle_exceptions
def main ():
    return report_main()

@handle_exceptions
def report_main ():
    options, args = parser.parse_args()
    report = get_report(args)
    report(user=get_current_user(), projects=options.projects, users=options.users,
        resources=options.resources, after=options.after,
        before=options.before, extra=options.extra)

def get_report (args):
    try:
        requested_report = args[0]
    except IndexError:
        report = "usage"
    else:
        possible_reports = [
            report for report in reports_available
            if report.startswith(requested_report)]
        if not possible_reports:
            raise UnknownReport(requested_report)
        elif len(possible_reports) > 1:
            raise UnknownReport("could be %s" % ", ".join(possible_reports))
        report = possible_reports[0]
    if report in ("use", "usage"):
        return views.print_usage
    elif report == "projects":
        return views.print_projects
    elif report == "allocations":
        return views.print_allocations
    elif report == "charges":
        return views.print_charges
    else:
        raise UnknownReport(report)
