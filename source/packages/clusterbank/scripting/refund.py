import sys
import os
from optparse import OptionParser

import clusterbank
import clusterbank.model
from clusterbank.model import Request, Refund
from clusterbank.scripting import options, verify_configured, \
    ArgumentParser, MissingArgument, InvalidArgument, ExtraArguments


parser = OptionParser(
    version = clusterbank.__version__,
    usage = os.linesep.join(["",
        "    %prog <charge> <time> [options]",
        "    %prog --list [options]",
    ]),
    description = "Refund time previously charged against a project on a resource.",
)
parser.add_option(options.list.having(help="list active refunds"))
parser.add_option(options.project.having(help="list refunds for PROJECT"))
parser.add_option(options.resource.having(help="list refunds for RESOURCE"))
parser.add_option(options.lien.having(help="list refunds under LIEN"))
parser.add_option(options.charge.having(help="post or list refunds of CHARGE"))
parser.add_option(options.time.having(help="refund TIME"))
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
        
        # At this point, no more arguments are used.
        arg_parser.verify_empty()
        
        refunds = Refund.query()
        
        if opts.charge:
            refunds = refunds.filter_by(charge=opts.charge)
        
        refunds = refunds.join("charge")
        
        if opts.lien:
            refunds = refunds.filter_by(lien=opts.lien)
        
        refunds = refunds.join(["charge", "lien", "allocation", "request"])
        
        if opts.project:
            refunds = refunds.filter_by(project=opts.project)
        
        if opts.resource:
            refunds = refunds.filter_by(resource=opts.resource)
        
        refunds = (
            refund for refund in refunds
            if refund.active
        )
        return refunds
    
    else:
        # create options:
        # charge -- charge to refund (required)
        # time -- amount of refund (required)
        # comment -- reason for refund
        
        kwargs = dict(
            charge = arg_parser.get(options.charge, opts, arg="charge"),
            time = arg_parser.get(options.time, opts, arg="time"),
        )
        
        arg_parser.verify_empty()
        
        if opts.comment is not None:
            kwargs['comment'] = opts.comment
        
        refund = Refund(**kwargs)
        
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        
        return [refund]
