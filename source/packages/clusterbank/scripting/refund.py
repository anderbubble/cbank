import sys
import os

import clusterbank
import clusterbank.model
from clusterbank import scripting
import clusterbank.scripting.options
from clusterbank.model import Request, Refund
from clusterbank.scripting import \
    MissingArgument, InvalidArgument, ExtraArguments


class OptionParser (scripting.OptionParser):
    
    standard_option_list = [
        scripting.options.list.having(help="list active refunds"),
        scripting.options.user.having(help="post refund as USER"),
        scripting.options.project.having(help="list refunds for PROJECT"),
        scripting.options.resource.having(help="list refunds for RESOURCE"),
        scripting.options.lien.having(help="list refunds under LIEN"),
        scripting.options.charge.having(help="post or list refunds of CHARGE"),
        scripting.options.time.having(help="refund TIME"),
        scripting.options.comment.having(help="misc. NOTES"),
    ]
    
    __defaults__ = dict(
        list = False,
    )
    
    __version__ = clusterbank.__version__
    usage = os.linesep.join(["",
        "    %prog <user> <charge> <time> [options]",
        "    %prog <user> --list [options]",
    ])
    __description__ = "Refund time previously charged against a project on a resource."


def run (argv=None):
    if argv is None:
        argv = sys.argv
    
    scripting.verify_configured()
    
    parser = OptionParser(prog=os.path.basename(argv[0]))
    options, args = parser.parse_args(args=argv[1:])
    arg_parser = scripting.ArgumentParser(args)
    
    user = arg_parser.get(scripting.options.user, options)
    
    if options.list:
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        refunds = Refund.query
        
        if options.charge:
            refunds = refunds.filter_by(charge=options.charge)
        
        refunds = refunds.join("charge")
        
        if options.lien:
            refunds = refunds.filter_by(lien=options.lien)
        
        refunds = refunds.join(["charge", "lien", "allocation", "request"])
        
        if options.project:
            refunds = refunds.filter_by(project=options.project)
        else:
            project_ids = [project.id for project in user.projects]
            refunds = refunds.filter(Request.c.project_id.in_(project_ids))
        
        if options.resource:
            refunds = refunds.filter_by(resource=options.resource)
        
        refunds = (
            refund for refund in refunds
            if refund.active
        )
        return refunds
    
    else:
        # create options:
        # user -- user performing the refund (required)
        # charge -- charge to refund (required)
        # time -- amount of refund (required)
        # comment -- reason for refund
        
        charge = arg_parser.get(scripting.options.charge, options)
        time = arg_parser.get(scripting.options.time, options)
        
        kwargs = dict(
            charge = charge,
            time = time,
        )
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        if options.comment is not None:
            kwargs['comment'] = options.comment
        
        refund = user.refund(**kwargs)
        
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        
        return [refund]
