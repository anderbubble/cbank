import sys
import os
from optparse import OptionParser

import clusterbank
import clusterbank.model
from clusterbank.model import Project, Request, Allocation, Hold
from clusterbank.scripting import options, verify_configured

parser = OptionParser(
    version = clusterbank.__version__,
    usage = os.linesep.join(["",
        "    %prog <allocation> <amount> [options]",
        "    %prog <amount> -p <project> -r <resource> [options]",
        "    %prog --list [options]",
    ]),
    description = "Post a hold against allocations for a project on a resource.",
)    
parser.add_option(options.list.having(help="list open active holds"))
parser.add_option(options.allocation.having(help="post hold against ALLOCATION"))
parser.add_option(options.project.having(help="post hold against or list holds for PROJECT"))
parser.add_option(options.resource.having(help="post hold against or list holds for RESOURCE"))
parser.add_option(options.amount.having(help="post hold for AMOUNT"))
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
        holds = Hold.query()
        if opts.allocation:
            holds = holds.filter(Hold.allocation==opts.allocation)
        holds = holds.join(["allocation", "request"])
        if opts.project:
            holds = holds.filter(Request.project==opts.project)
        if opts.resource:
            holds = holds.filter(Request.resource==opts.resource)
        holds = (
            hold for hold in holds
            if hold.active and hold.open
        )
        return holds
    
    else:
        if opts.amount is None:
            raise Exception("must specify an amount")
        if not opts.allocation:
            if not opts.project or not opts.resource:
                raise Exception("must specify an allocation")
        kwargs = dict(
            comment = opts.comment,
            amount = opts.amount,
        )
        if opts.allocation:
            holds = [Hold(allocation=opts.allocation, **kwargs)]
        else:
            allocations = Allocation.query.join("request")
            allocations = allocations.filter(Request.project==opts.project)
            allocations = allocations.filter(Request.resource==opts.resource)
            allocations = allocations.order_by([Allocation.expiration, Allocation.datetime])
            allocations = [
                allocation for allocation in allocations
                if allocation.active
            ]
            holds = Hold.distributed(allocations, **kwargs)
        
        clusterbank.model.Session.commit()
        clusterbank.model.Session.flush()
        return holds
