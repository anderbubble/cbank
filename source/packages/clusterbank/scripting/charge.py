import sys
import os

import elixir

import clusterbank
from clusterbank import scripting
from clusterbank.models import Project, Request, Charge
from clusterbank.scripting import \
    ScriptingError, NotPermitted, \
    MissingArgument, InvalidArgument, ExtraArguments


class OptionParser (scripting.OptionParser):
    
    standard_option_list = [
        scripting.OPTIONS['list'].having(help="list active charges"),
        scripting.OPTIONS['user'].having(help="post charge as or list charges for USER"),
        scripting.OPTIONS['project'].having(help="list charges for PROJECT"),
        scripting.OPTIONS['resource'].having(help="list charges against RESOURCE"),
        scripting.OPTIONS['liens'].having(help="post charges against LIENS"),
        scripting.OPTIONS['time'].having(help="charge TIME against liens"),
        scripting.OPTIONS['explanation'].having(help="misc. NOTES"),
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
        # user -- user whose project to list charges of (required)
        # project -- project to list charges of
        # resource -- resource to list charges for
        # liens -- liens to list charges for
        
        # At this point, no more arguments are used.
        try:
            arg_parser.verify_empty()
        except arg_parser.NotEmpty, e:
            raise ExtraArguments(e)
        
        charges = Charge.query()
        
        if options.liens:
            lien_ids = [lien.id for lien in options.liens]
            charges = charges.filter(Charge.c.lien_id.in_(*lien_ids))
        
        charges = charges.join(["lien", "allocation", "request"])
        
        if options.project:
            charges = charges.filter_by(project=options.project)
        else:
            project_ids = [project.id for project in user.projects]
            charges = charges.filter(Request.c.project_id.in_(*project_ids))
        
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
        # explanation -- explanation for the charge
        
        try:
            liens = arg_parser.get(scripting.OPTIONS['liens'], options)
        except arg_parser.NoValue, e:
            raise MissingArgument("%s: liens" % e)
        except arg_parser.InvalidArgument, e:
            raise InvalidArgument("%s (lien not found)" % e)
        try:
            time = arg_parser.get(scripting.OPTIONS['time'], options)
        except arg_parser.NoValue, e:
            raise MissingArgument("%s: time" % e)
        except arg_parser.InvalidArgument, e:
            raise InvalidArgument(e)
        
        kwargs = dict(
            liens = liens,
            time = time,
        )
        
        # At this point, no more arguments are used.
        try:
            arg_parser.verify_empty()
        except arg_parser.NotEmpty, e:
            raise ExtraArguments(e)
        
        if options.explanation is not None:
            kwargs['explanation'] = options.explanation
        
        charges = user.charge(**kwargs)
        try:
            elixir.objectstore.flush()
        except (user.NotPermitted, Project.InsufficientFunds), e:
            raise NotPermitted(e)
        except ValueError, e:
            raise InvalidArgument(e)
        
        return charges
