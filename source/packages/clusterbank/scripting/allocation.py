import sys
import os

import clusterbank
import clusterbank.model
from clusterbank import scripting
from clusterbank.model import Request, Allocation
from clusterbank.scripting import \
    ScriptingError, NotPermitted, \
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
    
    try:
        user = arg_parser.get(scripting.OPTIONS['user'], options)
    except arg_parser.NoValue, e:
        raise MissingArgument("%s: user" % e)
    except arg_parser.InvalidArgument, e:
        raise InvalidArgument("%s (user not found)" % e)
    
    if options.list:
        # list options:
        # user -- user whose project to list allocations for (required)
        # project -- project to list allocations for
        # resource -- resource to list allocations for
        # request -- request to list allocations for
        
        # At this point, no more arguments are used.
        try:
            arg_parser.verify_empty()
        except arg_parser.NotEmpty, e:
            raise ExtraArguments(e)
        
        allocations = Allocation.query()
        
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
        
        try:
            request = arg_parser.get(scripting.OPTIONS['request'], options)
        except arg_parser.NoValue, e:
            raise MissingArgument("%s: request" % e)
        except arg_parser.InvalidArgument, e:
            raise InvalidArgument("%s (request not found)" % e)
        try:
            start = arg_parser.get(scripting.OPTIONS['start'], options)
        except arg_parser.NoValue, e:
            raise MissingArgument("%s: start" % e)
        except arg_parser.InvalidArgument, e:
            raise InvalidArgument(e)
        try:
            expiration = arg_parser.get(scripting.OPTIONS['expiration'], options)
        except arg_parser.NoValue, e:
            raise MissingArgument("%s: expiration" % e)
        except arg_parser.InvalidArgument, e:
            raise InvalidArgument(e)
        
        try:
            arg_parser.verify_empty()
        except arg_parser.NotEmpty, e:
            raise ExtraArguments(e)
        
        # Create the new allocation.
        kwargs = dict(
            request = request,
            start = start,
            expiration = expiration,
        )
        if options.time is not None:
            kwargs['time'] = options.time
        
        try:
            allocation = user.allocate(**kwargs)
        except user.NotPermitted, e:
            raise NotPermitted(e)
        except ValueError, e:
            raise InvalidArgument(e)
        
        # Set up a line of credit.
        if options.credit is not None:
            kwargs = dict(
                resource = allocation.resource,
                project = allocation.project,
                start = allocation.start,
                comment = allocation.comment,
                time = options.credit,
            )
            try:
                credit_limit = user.allocate_credit(**kwargs)
            except user.NotPermitted, e:
                raise NotPermitted(e)
            except ValueError, e:
                raise InvalidArgument(e)
        
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        
        return [allocation]
