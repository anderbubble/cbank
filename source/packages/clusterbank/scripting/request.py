import sys
import os
from optparse import OptionParser

import clusterbank
import clusterbank.model
from clusterbank.model import Request
from clusterbank.scripting import options, verify_configured, \
    ArgumentParser, MissingArgument, InvalidArgument, ExtraArguments

parser = OptionParser(
    version = clusterbank.__version__,
    usage = os.linesep.join(["",
        "    %prog <project> <resource> <time> [options]",
        "    %prog --list [options]",
    ]),
    description = "Request an allocation of time for a project on a resource.",
)
parser.add_option(options.list.having(help="list open requests"))
parser.add_option(options.project.having(help="request time for or list requests for PROJECT"))
parser.add_option(options.resource.having(help="request time on or list requests for RESOURCE"))
parser.add_option(options.time.having(help="request amount of TIME"))
parser.add_option(options.start.having(help="request allocation to begin on DATE"))
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
        # project -- project to list requests for
        # resource -- resource to list requests for
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        requests = Request.query()
        
        if opts.project:
            requests = requests.filter_by(project=opts.project)
        
        if opts.resource:
            requests = requests.filter_by(resource=opts.resource)
        
        requests = (
            request for request in requests
            if request.open
        )
        return requests
    
    else:
        # create options:
        # project -- project requesting for (required)
        # resource -- resource requesting time on (required)
        # start -- when time is needed
        # time -- amount of time requested (required)
        # comment -- reason for request
        
        project = arg_parser.get(options.project, opts, arg="project")
        resource = arg_parser.get(options.resource, opts, arg="resource")
        time = arg_parser.get(options.time, opts, arg="time")
        
        arg_parser.verify_empty()
        
        kwargs = dict(
            project = project,
            resource = resource,
            time = time,
        )
        if opts.start is not None:
            kwargs['start'] = opts.start
        if opts.comment is not None:
            kwargs['comment'] = opts.comment
        
        request = Request(**kwargs)
        
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        
        return [request]
