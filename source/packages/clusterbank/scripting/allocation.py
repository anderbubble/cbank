import sys
import os

import clusterbank
import clusterbank.model
from clusterbank import scripting
from clusterbank.model import Request, Allocation
from clusterbank.scripting import \
    MissingArgument, InvalidArgument, ExtraArguments


class OptionParser (scripting.OptionParser):
    
    standard_option_list = [
        scripting.OPTIONS['list'].having(help="list active allocations"),
        scripting.OPTIONS['user'].having(help="allocate as or list allocations for USER"),
        scripting.OPTIONS['project'].having(help="list allocations for PROJECT"),
        scripting.OPTIONS['resource'].having(help="list allocations for RESOURCE"),
        scripting.OPTIONS['request'].having(help="allocate for REQUEST"),
        scripting.OPTIONS['time'].having(help="allocate TIME"),
        scripting.OPTIONS['credit'].having(help="PROJECT can use up to LIMIT negative time"),
        scripting.OPTIONS['start'].having(help="TIME becomes available on DATE"),
        scripting.OPTIONS['expiration'].having(help="TIME expires on DATE"),
        scripting.OPTIONS['comment'].having(help="misc. NOTES"),
    ]
    
    __defaults__ = dict(
        list = False,
    )
    
    __version__ = clusterbank.__version__
    __usage__ = os.linesep.join(["",
        "    %prog <user> <request> <start> <expiration> [options]",
        "    %prog <user> --list [options]",
    ])
    __description__ = "Allocate time on a resource for a project."


def run (argv=sys.argv):
    parser = OptionParser(prog=os.path.basename(argv[0]))
    options, args = parser.parse_args(args=argv[1:])
    arg_parser = scripting.ArgumentParser(args)
    
    user = arg_parser.get(scripting.OPTIONS['user'], options)
    
    if options.list:
        # list options:
        # user -- user whose project to list allocations for (required)
        # project -- project to list allocations for
        # resource -- resource to list allocations for
        # request -- request to list allocations for
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        allocations = Allocation.query
        
        if options.request:
            allocations = allocations.filter_by(request=options.request)
        
        allocations = allocations.join("request")
        
        if options.project:
            allocations = allocations.filter_by(project=options.project)
        else:
            project_ids = [project.id for project in user.projects]
            allocations = allocations.filter(Request.c.project_id.in_(project_ids))
        
        if options.resource:
            allocations = allocations.filter_by(resource=options.resource)
        
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
        
        request = arg_parser.get(scripting.OPTIONS['request'], options)
        start = arg_parser.get(scripting.OPTIONS['start'], options)
        expiration = arg_parser.get(scripting.OPTIONS['expiration'], options)
        
        arg_parser.verify_empty()
        
        # Create the new allocation.
        kwargs = dict(
            request = request,
            start = start,
            expiration = expiration,
        )
        if options.time is not None:
            kwargs['time'] = options.time
        
        allocation = user.allocate(**kwargs)
        
        # Set up a line of credit.
        if options.credit is not None:
            kwargs = dict(
                resource = allocation.resource,
                project = allocation.project,
                start = allocation.start,
                comment = allocation.comment,
                time = options.credit,
            )
            credit_limit = user.allocate_credit(**kwargs)
        
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        
        return [allocation]
