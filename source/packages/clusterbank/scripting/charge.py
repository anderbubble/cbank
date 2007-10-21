import sys
import os

import clusterbank
import clusterbank.model
from clusterbank import scripting
from clusterbank.model import Project, Request, Charge
from clusterbank.scripting import \
    verify_configured, \
    MissingArgument, InvalidArgument, ExtraArguments


class OptionParser (scripting.OptionParser):
    
    standard_option_list = [
        scripting.OPTIONS['list'].having(help="list active charges"),
        scripting.OPTIONS['user'].having(help="post charge as or list charges for USER"),
        scripting.OPTIONS['project'].having(help="list charges for PROJECT"),
        scripting.OPTIONS['resource'].having(help="list charges against RESOURCE"),
        scripting.OPTIONS['liens'].having(help="post charges against LIENS"),
        scripting.OPTIONS['time'].having(help="charge TIME against liens"),
        scripting.OPTIONS['comment'].having(help="misc. NOTES"),
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
    
    verify_configured()
    
    parser = OptionParser(prog=os.path.basename(argv[0]))
    options, args = parser.parse_args(args=argv[1:])
    arg_parser = scripting.ArgumentParser(args)
    
    user = arg_parser.get(scripting.OPTIONS['user'], options)
    
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
        
        liens = arg_parser.get(scripting.OPTIONS['liens'], options)
        time = arg_parser.get(scripting.OPTIONS['time'], options)
        
        kwargs = dict(
            liens = liens,
            time = time,
        )
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        if options.comment is not None:
            kwargs['comment'] = options.comment
        
        charges = user.charge(**kwargs)
        
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        
        return charges
