import sys
import os
from optparse import OptionParser

import clusterbank
import clusterbank.model
from clusterbank.model import Request
from clusterbank.scripting import options, verify_configured

parser = OptionParser(
    version = clusterbank.__version__,
    usage = os.linesep.join(["",
        "    %prog <project> <resource> <amount> [options]",
        "    %prog --list [options]",
    ]),
    description = "Request an allocation for a project on a resource.",
)
parser.add_option(options.list.having(help="list open requests"))
parser.add_option(options.project.having(help="request amount for or list requests for PROJECT"))
parser.add_option(options.resource.having(help="request amount of or list requests for RESOURCE"))
parser.add_option(options.amount.having(help="request AMOUNT"))
parser.add_option(options.start.having(help="request allocation to begin on DATE"))
parser.add_option(options.comment.having(help="misc. NOTES"))
parser.set_defaults(list=False)

def run (argv=None):
    if argv is None:
        argv = sys.argv
    
    verify_configured()
    parser.prog = os.path.basename(argv[0])
    opts, args = parser.parse_args(args=argv[1:])
    if args:
        raise Exception("unknown argument(s): %s" % ", ".join(args))
    
    if opts.list:
        requests = Request.query()
        if opts.project:
            requests = requests.filter(Request.project==opts.project)
        if opts.resource:
            requests = requests.filter(Request.resource==opts.resource)
        requests = (
            request for request in requests
            if request.open
        )
        return requests
    
    else:
        if not opts.project:
            raise Exception("must specify a project")
        if not opts.resource:
            raise Exception("must specify a resource")
        if opts.amount is None:
            raise Exception("must specify an amount")
        clusterbank.model.Session.begin()
        request = Request(
            project = opts.project,
            resource = opts.resource,
            amount = opts.amount,
            start = opts.start,
            comment = opts.comment,
        )
        clusterbank.model.Session.commit()
        clusterbank.model.Session.flush()
        return [request]
