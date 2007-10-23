import sys
import os

import clusterbank
import clusterbank.model
from clusterbank import scripting
import clusterbank.scripting.options
from clusterbank.model import Project, Request, Charge
from clusterbank.scripting import \
    MissingArgument, InvalidArgument, ExtraArguments


class OptionParser (scripting.OptionParser):
    
    standard_option_list = [
        scripting.options.list.having(help="list active charges"),
        scripting.options.user.having(help="post charge as or list charges for USER"),
        scripting.options.project.having(help="list charges for PROJECT"),
        scripting.options.resource.having(help="list charges against RESOURCE"),
        scripting.options.liens.having(help="post charges against LIENS"),
        scripting.options.time.having(help="charge TIME against liens"),
        scripting.options.comment.having(help="misc. NOTES"),
    ]
    
    __defaults__ = dict(
        list = False,
    )
    
    __version__ = clusterbank.__version__
    __usage__ = os.linesep.join(["",
        "    %prog <user> <liens> <time> [options]",
        "    %prog <user> --list [options]",
    ])
    __description__ = "Charge time previously liened against a project on a resource."


def run (argv=None):
    if argv is None:
        argv = sys.argv
    
    scripting.verify_configured()
    
    parser = OptionParser(prog=os.path.basename(argv[0]))
    options, args = parser.parse_args(args=argv[1:])
    arg_parser = scripting.ArgumentParser(args)
    
    user = arg_parser.get(scripting.options.user, options)
    
    if options.list:
        # list options:
        # user -- user whose project to list charges of (required)
        # project -- project to list charges of
        # resource -- resource to list charges for
        # liens -- liens to list charges for
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        charges = Charge.query
        
        if options.liens:
            lien_ids = [lien.id for lien in options.liens]
            charges = charges.filter(Charge.c.lien_id.in_(lien_ids))
        
        charges = charges.join(["lien", "allocation", "request"])
        
        if options.project:
            charges = charges.filter_by(project=options.project)
        else:
            project_ids = [project.id for project in user.projects]
            charges = charges.filter(Request.c.project_id.in_(project_ids))
        
        if options.resource:
            charges = charges.filter_by(resource=options.resource)
        
        charges = (
            charge for charge in charges
            if charge.active
        )
        return charges
        
    else:
        # create options:
        # user -- user doing the charge (required)
        # liens -- lien(s) to charge (required)
        # time -- amount of time to charge (required)
        # comment -- comment for the charge
        
        kwargs = dict(
            liens = arg_parser.get(scripting.options.liens, options),
            time = arg_parser.get(scripting.options.time, options),
            poster = user,
        )
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        if options.comment is not None:
            kwargs['comment'] = options.comment
        
        charges = Charge.distributed(**kwargs)
        
        clusterbank.model.Session.commit()
        clusterbank.model.Session.flush()
        
        return charges
