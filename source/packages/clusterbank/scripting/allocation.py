import sys
import os
from optparse import OptionParser

import clusterbank
import clusterbank.model
from clusterbank.model import Request, Allocation, CreditLimit
from clusterbank.scripting import options, verify_configured


parser = OptionParser(
    version = clusterbank.__version__,
    usage = os.linesep.join(["",
        "    %prog <request> <start> <expiration> [options]",
        "    %prog --list [options]",
    ]),
    description = "Allocate amount on a resource for a project.",
)
parser.add_option(options.list.having(help="list active allocations"))
parser.add_option(options.project.having(help="list allocations for PROJECT"))
parser.add_option(options.resource.having(help="list allocations for RESOURCE"))
parser.add_option(options.request.having(help="allocate for REQUEST"))
parser.add_option(options.amount.having(help="allocate AMOUNT"))
parser.add_option(options.credit.having(help="PROJECT can use up to LIMIT negative amount"))
parser.add_option(options.start.having(help="AMOUNT becomes available on DATE"))
parser.add_option(options.expiration.having(help="AMOUNT expires on DATE"))
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
        allocations = Allocation.query()
        if opts.request:
            allocations = allocations.filter(Allocation.request==opts.request)
        allocations = allocations.join("request")
        if opts.project:
            allocations = allocations.filter(Request.project==opts.project)
        if opts.resource:
            allocations = allocations.filter(Request.resource==opts.resource)
        allocations = (
            allocation for allocation in allocations
            if allocation.active
        )
        return allocations
    
    else:
        if not opts.request:
            raise Exception("must specify a request")
        if not opts.start:
            raise Exception("must specify a start date")
        if not opts.expiration:
            raise Exception("must specify an expiration date")
        allocation = Allocation(
            request = opts.request,
            start = opts.start,
            expiration = opts.expiration,
            amount = opts.amount,
        )
        if opts.credit is not None:
            credit_limit = CreditLimit(
                resource = allocation.resource,
                project = allocation.project,
                start = allocation.start,
                comment = allocation.comment,
                amount = opts.credit,
            )
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        return [allocation]
