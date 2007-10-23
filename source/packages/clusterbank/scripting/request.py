import sys
import os

import clusterbank
import clusterbank.model
from clusterbank.model import Request
from clusterbank import scripting
import clusterbank.scripting.options
from clusterbank.scripting import \
    MissingArgument, InvalidArgument, ExtraArguments


class OptionParser (scripting.OptionParser):

    standard_option_list = [
        scripting.options.list.having(help="list open requests"),
        scripting.options.user.having(help="request as or list open requests for USER"),
        scripting.options.project.having(help="request time for or list requests for PROJECT"),
        scripting.options.resource.having(help="request time on or list requests for RESOURCE"),
        scripting.options.time.having(help="request amount of TIME"),
        scripting.options.start.having(help="request allocation to begin on DATE"),
        scripting.options.comment.having(help="misc. NOTES"),
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


def run (argv=None):
    if argv is None:
        argv = sys.argv
    
    scripting.verify_configured()
    
    parser = OptionParser(prog=os.path.basename(argv[0]))
    options, args = parser.parse_args(args=argv[1:])
    arg_parser = scripting.ArgumentParser(args)
    
    user = arg_parser.get(scripting.options.user, options, arg="user")
    
    if options.list:
        # list options:
        # user -- user whose projects to list requests for (required)
        # project -- project to list requests for
        # resource -- resource to list requests for
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        requests = Request.query
        
        if options.project:
            requests = requests.filter_by(project=options.project)
        else:
            project_ids = [project.id for project in user.projects]
            requests = requests.filter(Request.c.project_id.in_(project_ids))
        
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
        # comment -- reason for request
        
        project = arg_parser.get(scripting.options.project, options, arg="project")
        resource = arg_parser.get(scripting.options.resource, options, arg="resource")
        time = arg_parser.get(scripting.options.time, options, arg="time")
        
        arg_parser.verify_empty()
        
        kwargs = dict(
            poster = user,
            project = project,
            resource = resource,
            time = time,
        )
        if options.start is not None:
            kwargs['start'] = options.start
        if options.comment is not None:
            kwargs['comment'] = options.comment
        
        request = Request(**kwargs)
        
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        
        return [request]
