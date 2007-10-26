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
        "    %prog <user> <allocation> <time> [options]",
        "    %prog <user> <time> -p <project> -r <resource> [options]",
        "    %prog <user> --list [options]",
    ]),
    description = "Post a lien against allocations for a project on a resource.",
)    
parser.add_option(options.list.having(help="list open active liens"))
parser.add_option(options.user.having(help="post lien as or list liens for USER"))
parser.add_option(options.allocation.having(help="post lien against ALLOCATION"))
parser.add_option(options.project.having(help="post lien against or list liens for PROJECT"))
parser.add_option(options.resource.having(help="post lien against or list liens for RESOURCE"))
parser.add_option(options.time.having(help="post lien for TIME"))
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
        # user -- user whose projects to list liens on (required)
        # project -- project to list liens on
        # resource -- resource to list liens for
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        liens = Lien.query
        
        if opts.allocation:
            liens = liens.filter_by(allocation=opts.allocation)
        
        liens = liens.join(["allocation", "request"])
        
        if opts.project:
            liens = liens.filter_by(project=opts.project)
        else:
            project_ids = [project.id for project in user.projects]
            liens = liens.filter(Request.c.project_id.in_(project_ids))
        
        if opts.resource:
            liens = liens.filter_by(resource=opts.resource)
        
        liens = (
            lien for lien in liens
            if lien.active and lien.open
        )
        return liens
    
    else:
        # create options:
        # user -- user creating the lien (required)
        # allocation -- allocation for the lien (required for standard lien)
        # project -- project of the lien (required for smart lien)
        # resource -- resource of the lien (required for smart lien)
        # time -- maximum charge of the lien
        # comment -- comment for the lien
        
        kwargs = dict(
            poster = user,
            comment = opts.comment,
        )
        
        try:
            kwargs['allocation'] = arg_parser.get(options.allocation, opts, arg="allocation")
        except MissingArgument:
            if not (opts.project and opts.resource):
                raise
            kwargs['time'] = arg_parser.get(options.time, opts, arg="time")
            arg_parser.verify_empty()
            allocations = Allocation.query.join("request").filter_by(project=opts.project, resource=opts.resource)
            allocations = allocations.order_by([Allocation.c.expiration, Allocation.c.datetime])
            allocations = [
                allocation for allocation in allocations
                if allocation.active
            ]
            liens = Lien.distributed(allocations, **kwargs)
        else:
            kwargs['time'] = arg_parser.get(options.time, opts, arg="time")
            arg_parser.verify_empty()
            return [Lien(**kwargs)]
        
        clusterbank.model.Session.commit()
        clusterbank.model.Session.flush()
        
        return liens
