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

__all__ = ["main", "report_main"]

config = ConfigParser.SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

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

@handle_exceptions
def main ():
    return report_main()

@handle_exceptions
def report_main ():
    parser = build_report_parser()
    options, args = parser.parse_args()
    report = get_report(args)
    report(user=get_current_user(), projects=options.projects, users=options.users,
        resources=options.resources, after=options.after,
        before=options.before, extra=options.extra)

def get_report (args):
    try:
        requested_report = args[0]
    except IndexError:
        return views.print_usage
    else:
        possible_reports = []
        if "usage".startswith(requested_report):
            possible_reports.append(views.print_usage)
        if "projects".startswith(requested_report):
            possible_reports.append(views.print_projects)
        if "allocations".startswith(requested_report):
            possible_reports.append(views.print_allocations)
        if "charges".startswith(requested_report):
            possible_reports.append(views.print_charges)
        if len(possible_reports) != 1:
            raise exceptions.UnknownReport(requested_report)
        else:
            return possible_reports[0]

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

def build_report_parser ():
    
    usage_str = os.linesep.join([
        "cbank [options] [report]",
        "",
        "reports:",
        "  usage, projects, allocations, charges"])

    version_str = "cbank %s" % clusterbank.__version__
    
    parser = optparse.OptionParser(usage=usage_str, version=version_str)
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
    return parser
