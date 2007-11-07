import sys
import os
from optparse import OptionParser

import clusterbank
import clusterbank.model
from clusterbank.model import Request, Allocation, CreditLimit
from clusterbank.scripting import options, verify_configured, \
    ArgumentParser, MissingArgument, InvalidArgument, ExtraArguments


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
    arg_parser = ArgumentParser(args)
    
    if opts.list:
        # list options:
        # project -- project to list allocations for
        # resource -- resource to list allocations for
        # request -- request to list allocations for
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        allocations = Allocation.query()
        
        if opts.request:
            allocations = allocations.filter_by(request=opts.request)
        
        allocations = allocations.join("request")
        
        if opts.project:
            allocations = allocations.filter_by(project=opts.project)
        
        if opts.resource:
            allocations = allocations.filter_by(resource=opts.resource)
        
        allocations = (
            allocation for allocation in allocations
            if allocation.active
        )
        return allocations
    
    else:
        # create options:
        # request -- request to allocate amount for (required)
        # amount -- amount to allocate
        # start -- date the allocation becomes active (required)
        # expiration -- specify an expiration date (required)
        # comment -- comment of the allocation
        
        request = arg_parser.get(options.request, opts, arg="request")
        start = arg_parser.get(options.start, opts, arg="start")
        expiration = arg_parser.get(options.expiration, opts, arg="expiration")
        
        arg_parser.verify_empty()
        
        # Create the new allocation.
        kwargs = dict(
            request = request,
            start = start,
            expiration = expiration,
        )
        if opts.amount is not None:
            kwargs['amount'] = opts.amount
        
        allocation = Allocation(**kwargs)
        
        # Set up a line of credit.
        if opts.credit is not None:
            kwargs = dict(
                resource = allocation.resource,
                project = allocation.project,
                start = allocation.start,
                comment = allocation.comment,
                amount = opts.credit,
            )
            credit_limit = CreditLimit(**kwargs)
        
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        
        return [allocation]
