import sys
import os

import elixir

import clusterbank
from clusterbank.models import Request
from clusterbank import scripting
from clusterbank.scripting import ArgumentParser, \
    ScriptingError, NotPermitted, \
    MissingArgument, InvalidArgument, ExtraArguments


class OptionParser (scripting.OptionParser):

    standard_option_list = [
        scripting.OPTIONS['list'].having(help="list open requests"),
        scripting.OPTIONS['user'].having(help="request as or list open requests for USER"),
        scripting.OPTIONS['project'].having(help="request time for or list requests for PROJECT"),
        scripting.OPTIONS['resource'].having(help="request time on or list requests for RESOURCE"),
        scripting.OPTIONS['time'].having(help="request amount of TIME"),
        scripting.OPTIONS['start'].having(help="request allocation to begin on DATE"),
        scripting.OPTIONS['explanation'].having(help="misc. NOTES"),
    ]
    
    __defaults__ = dict(
        list = False,
    )
    
    __version__ = clusterbank.__version__
    __usage__ = os.linesep.join(["",
        "    %prog <user> <project> <resource> <time> [options]",
        "    %prog <user> --list [options]",
    ])
    __description__ = "Request an allocation of time for a project on a resource."


def run (argv=sys.argv):
    parser = OptionParser(prog=os.path.basename(argv[0]))
    options, args = parser.parse_args(args=argv[1:])
    arg_parser = ArgumentParser(args)
    
    try:
        user = arg_parser.get(scripting.OPTIONS['user'], options)
    except arg_parser.NoValue, e:
        raise MissingArgument("%s: user" % e)
    except arg_parser.InvalidArgument, e:
        raise InvalidArgument("%s (user not found)" % e)
    
    if options.list:
        # list options:
        # user -- user whose projects to list requests for (required)
        # project -- project to list requests for
        # resource -- resource to list requests for
        
        # At this point, no more arguments are used.
        try:
            arg_parser.verify_empty()
        except arg_parser.NotEmpty, e:
            raise ExtraArguments(e)
        
        requests = Request.query()
        
        if options.project:
            requests = requests.filter_by(project=options.project)
        else:
            project_ids = [project.id for project in user.projects]
            requests = requests.filter(Request.c.project_id.in_(*project_ids))
        
        if options.resource:
            requests = requests.filter_by(resource=options.resource)
        
        requests = (
            request for request in requests
            if request.open
        )
        return requests
    
    else:
        # create options:
        # user -- user making request (required)
        # project -- project requesting for (required)
        # resource -- resource requesting time on (required)
        # start -- when time is needed
        # time -- amount of time requested (required)
        # explanation -- reason for request
        
        try:
            project = arg_parser.get(scripting.OPTIONS['project'], options)
        except arg_parser.NoValue, e:
            raise MissingArgument("%s: project" % e)
        except arg_parser.InvalidArgument, e:
            raise InvalidArgument("%s (project not found)" % e)
        try:
            resource = arg_parser.get(scripting.OPTIONS['resource'], options)
        except arg_parser.NoValue, e:
            raise MissingArgument("%s: resource" % e)
        except arg_parser.InvalidArgument, e:
            raise InvalidArgument("%s (resource not found)" % e)
        try:
            time = arg_parser.get(scripting.OPTIONS['time'], options)
        except arg_parser.NoValue, e:
            raise MissingArgument("%s: time" % e)
        except arg_parser.InvalidArgument, e:
            raise InvalidArgument(e)
        
        try:
            arg_parser.verify_empty()
        except arg_parser.NotEmpty, e:
            raise ExtraArguments(e)
        
        kwargs = dict(
            project = project,
            resource = resource,
            time = time,
        )
        if options.start is not None:
            kwargs['start'] = options.start
        if options.explanation is not None:
            kwargs['explanation'] = options.explanation
        
        request = user.request(**kwargs)
        try:
            elixir.objectstore.flush()
        except user.NotPermitted, e:
            raise NotPermitted(e)
        except ValueError, e:
            raise InvalidArgument(e)
        
        return [request]
