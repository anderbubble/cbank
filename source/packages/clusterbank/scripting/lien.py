import sys
import os
from optparse import OptionParser

import clusterbank
import clusterbank.model
from clusterbank.model import Project, Request, Allocation, Lien
from clusterbank.scripting import options, verify_configured, \
    ArgumentParser, MissingArgument, InvalidArgument, ExtraArguments

parser = OptionParser(
    version = clusterbank.__version__,
    usage = os.linesep.join(["",
        "    %prog <allocation> <amount> [options]",
        "    %prog <amount> -p <project> -r <resource> [options]",
        "    %prog --list [options]",
    ]),
    description = "Post a lien against allocations for a project on a resource.",
)    
parser.add_option(options.list.having(help="list open active liens"))
parser.add_option(options.allocation.having(help="post lien against ALLOCATION"))
parser.add_option(options.project.having(help="post lien against or list liens for PROJECT"))
parser.add_option(options.resource.having(help="post lien against or list liens for RESOURCE"))
parser.add_option(options.amount.having(help="post lien for AMOUNT"))
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
        # project -- project to list liens on
        # resource -- resource to list liens for
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        liens = Lien.query()
        
        if opts.allocation:
            liens = liens.filter(Lien.allocation==opts.allocation)
        
        liens = liens.join(["allocation", "request"])
        
        if opts.project:
            liens = liens.filter(Request.project==opts.project)
        
        if opts.resource:
            liens = liens.filter(Request.resource==opts.resource)
        
        liens = (
            lien for lien in liens
            if lien.active and lien.open
        )
        return liens
    
    else:
        # create options:
        # allocation -- allocation for the lien (required for standard lien)
        # project -- project of the lien (required for smart lien)
        # resource -- resource of the lien (required for smart lien)
        # amount -- maximum charge of the lien
        # comment -- comment for the lien
        
        kwargs = dict(
            comment = opts.comment,
        )
        
        try:
            kwargs['allocation'] = arg_parser.get(options.allocation, opts, arg="allocation")
        except MissingArgument:
            if not (opts.project and opts.resource):
                raise
            kwargs['amount'] = arg_parser.get(options.amount, opts, arg="amount")
            arg_parser.verify_empty()
            allocations = Allocation.query.join("request")
            allocations = allocations.filter(Request.project==opts.project)
            allocations = allocations.filter(Request.resource==opts.resource)
            allocations = allocations.order_by([Allocation.expiration, Allocation.datetime])
            allocations = [
                allocation for allocation in allocations
                if allocation.active
            ]
            liens = Lien.distributed(allocations, **kwargs)
        else:
            kwargs['amount'] = arg_parser.get(options.amount, opts, arg="amount")
            arg_parser.verify_empty()
            return [Lien(**kwargs)]
        
        clusterbank.model.Session.commit()
        clusterbank.model.Session.flush()
        
        return liens
