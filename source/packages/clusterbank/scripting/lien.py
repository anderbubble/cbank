import sys
import os

import clusterbank
from clusterbank import scripting
import clusterbank.model
from clusterbank.model import Project, Request, Lien
from clusterbank.scripting import \
    MissingArgument, InvalidArgument, ExtraArguments


class OptionParser (scripting.OptionParser):
    
    standard_option_list = [
        scripting.OPTIONS['list'].having(help="list open active liens"),
        scripting.OPTIONS['user'].having(help="post lien as or list liens for USER"),
        scripting.OPTIONS['allocation'].having(help="post lien against ALLOCATION"),
        scripting.OPTIONS['project'].having(help="post lien against or list liens for PROJECT"),
        scripting.OPTIONS['resource'].having(help="post lien against or list liens for RESOURCE"),
        scripting.OPTIONS['time'].having(help="post lien for TIME"),
        scripting.OPTIONS['comment'].having(help="misc. NOTES"),
    ]
    
    __defaults__ = dict(
        list = False,
    )
    
    __version__ = clusterbank.__version__
    __usage__ = os.linesep.join(["",
        "    %prog <user> <allocation> <time> [options]",
        "    %prog <user> <time> -p <project> -r <resource> [options]",
        "    %prog <user> --list [options]",
    ])
    __description__ = "Post a lien against allocations for a project on a resource."


def run (argv=sys.argv):
    parser = OptionParser(prog=os.path.basename(argv[0]))
    options, args = parser.parse_args(args=argv[1:])
    arg_parser = scripting.ArgumentParser(args)
    
    user = arg_parser.get(scripting.OPTIONS['user'], options)
    
    if options.list:
        # list options:
        # user -- user whose projects to list liens on (required)
        # project -- project to list liens on
        # resource -- resource to list liens for
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        liens = Lien.query
        
        if options.allocation:
            liens = liens.filter_by(allocation=options.allocation)
        
        liens = liens.join(["allocation", "request"])
        
        if options.project:
            liens = liens.filter_by(project=options.project)
        else:
            project_ids = [project.id for project in user.projects]
            liens = liens.filter(Request.c.project_id.in_(project_ids))
        
        if options.resource:
            liens = liens.filter_by(resource=options.resource)
        
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
        try:
            allocation = arg_parser.get(scripting.OPTIONS['allocation'], options)
        except MissingArgument:
            if not (options.project and options.resource):
                raise
            allocation = None
        
        time = arg_parser.get(scripting.OPTIONS['time'], options)
        
        arg_parser.verify_empty()
        
        kwargs = dict(
            project = options.project,
            resource = options.resource,
            allocation = allocation,
            time = time,
        )
        if options.comment is not None:
            kwargs['comment'] = options.comment
        
        lien = user.lien(**kwargs)
        
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        
        try:
            liens = list(lien)
        except TypeError:
            liens = [lien]
        
        return liens
