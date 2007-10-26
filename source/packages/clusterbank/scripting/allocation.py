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
        "    %prog <user> <request> <start> <expiration> [options]",
        "    %prog <user> --list [options]",
    ]),
    description = "Allocate time on a resource for a project.",
)
parser.add_option(options.list.having(help="list active allocations"))
parser.add_option(options.user.having(help="allocate as or list allocations for USER"))
parser.add_option(options.project.having(help="list allocations for PROJECT"))
parser.add_option(options.resource.having(help="list allocations for RESOURCE"))
parser.add_option(options.request.having(help="allocate for REQUEST"))
parser.add_option(options.time.having(help="allocate TIME"))
parser.add_option(options.credit.having(help="PROJECT can use up to LIMIT negative time"))
parser.add_option(options.start.having(help="TIME becomes available on DATE"))
parser.add_option(options.expiration.having(help="TIME expires on DATE"))
parser.add_option(options.comment.having(help="misc. NOTES"))
parser.set_defaults(list=False)


def run (argv=None):
    if argv is None:
        argv = sys.argv
    
    verify_configured()
    parser.prog = os.path.basename(argv[0])
    opts, args = parser.parse_args(args=argv[1:])
    arg_parser = ArgumentParser(args)
    
    user = arg_parser.get(options.user, opts, arg="user")
    
    if opts.list:
        # list options:
        # user -- user whose project to list allocations for (required)
        # project -- project to list allocations for
        # resource -- resource to list allocations for
        # request -- request to list allocations for
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        allocations = Allocation.query
        
        if opts.request:
            allocations = allocations.filter_by(request=opts.request)
        
        allocations = allocations.join("request")
        
        if opts.project:
            allocations = allocations.filter_by(project=opts.project)
        else:
            project_ids = [project.id for project in user.projects]
            allocations = allocations.filter(Request.c.project_id.in_(project_ids))
        
        if opts.resource:
            allocations = allocations.filter_by(resource=opts.resource)
        
        allocations = (
            allocation for allocation in allocations
            if allocation.active
        )
        return allocations
    
    else:
        # create options:
        # user -- user allocating the request (required)
        # request -- request to allocation time for (required)
        # time -- time to allocate
        # start -- date the allocation becomes active (required)
        # expiration -- specify an expiration date (required)
        # comment -- comment of the allocation
        
        request = arg_parser.get(options.request, opts, arg="request")
        start = arg_parser.get(options.start, opts, arg="start")
        expiration = arg_parser.get(options.expiration, opts, arg="expiration")
        
        arg_parser.verify_empty()
        
        # Create the new allocation.
        kwargs = dict(
            poster = user,
            request = request,
            start = start,
            expiration = expiration,
        )
        if opts.time is not None:
            kwargs['time'] = opts.time
        
        allocation = Allocation(**kwargs)
        
        # Set up a line of credit.
        if opts.credit is not None:
            kwargs = dict(
                poster = user,
                resource = allocation.resource,
                project = allocation.project,
                start = allocation.start,
                comment = allocation.comment,
                time = opts.credit,
            )
            credit_limit = CreditLimit(**kwargs)
        
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        
        return [allocation]
