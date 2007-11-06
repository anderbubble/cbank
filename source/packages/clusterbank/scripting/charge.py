import sys
import os
from optparse import OptionParser

import clusterbank
import clusterbank.model
from clusterbank.model import Project, Request, Charge
from clusterbank.scripting import options, verify_configured, \
    ArgumentParser, MissingArgument, InvalidArgument, ExtraArguments

parser = OptionParser(
    version = clusterbank.__version__,
    usage = os.linesep.join(["",
        "    %prog <liens> <time> [options]",
        "    %prog --list [options]",
    ]),
    description = "Charge time previously liened against a project on a resource.",
)
parser.add_option(options.list.having(help="list active charges"))
parser.add_option(options.project.having(help="list charges for PROJECT"))
parser.add_option(options.resource.having(help="list charges against RESOURCE"))
parser.add_option(options.liens.having(help="post charges against LIENS"))
parser.add_option(options.time.having(help="charge TIME against liens"))
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
        # project -- project to list charges of
        # resource -- resource to list charges for
        # liens -- liens to list charges for
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        charges = Charge.query()
        
        if opts.liens:
            lien_ids = [lien.id for lien in opts.liens]
            charges = charges.filter(Charge.c.lien_id.in_(lien_ids))
        
        charges = charges.join(["lien", "allocation", "request"])
        
        if opts.project:
            charges = charges.filter_by(project=opts.project)
        
        if opts.resource:
            charges = charges.filter_by(resource=opts.resource)
        
        charges = (
            charge for charge in charges
            if charge.active
        )
        return charges
        
    else:
        # create options:
        # liens -- lien(s) to charge (required)
        # time -- amount of time to charge (required)
        # comment -- comment for the charge
        
        kwargs = dict(
            liens = arg_parser.get(options.liens, opts, arg="liens"),
            time = arg_parser.get(options.time, opts, arg="time"),
        )
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        if opts.comment is not None:
            kwargs['comment'] = opts.comment
        
        charges = Charge.distributed(**kwargs)
        
        clusterbank.model.Session.commit()
        clusterbank.model.Session.flush()
        
        return charges
