import sys
import os
from optparse import OptionParser

import clusterbank
import clusterbank.model
from clusterbank.model import Project, Request, Charge
from clusterbank.scripting import options, verify_configured

parser = OptionParser(
    version = clusterbank.__version__,
    usage = os.linesep.join(["",
        "    %prog <holds> <amount> [options]",
        "    %prog --list [options]",
    ]),
    description = "Charge amount previously held against a project on a resource.",
)
parser.add_option(options.list.having(help="list active charges"))
parser.add_option(options.project.having(help="list charges for PROJECT"))
parser.add_option(options.resource.having(help="list charges against RESOURCE"))
parser.add_option(options.holds.having(help="post charges against LIENS"))
parser.add_option(options.amount.having(help="charge AMOUNT against holds"))
parser.add_option(options.comment.having(help="misc. NOTES"))
parser.set_defaults(list=False)

def run (argv=None):
    if argv is None:
        argv = sys.argv
    
    verify_configured()
    parser.prog = os.path.basename(argv[0])
    opts, args = parser.parse_args(args=argv[1:])
    if args:
        raise Exception("unknown argument(s): %s" % ", ".join(args))
    
    if opts.list:
        charges = Charge.query()
        if opts.holds:
            hold_ids = [hold.id for hold in opts.holds]
            charges = charges.filter(Charge.hold_id.in_(hold_ids))
        charges = charges.join(["hold", "allocation", "request"])
        if opts.project:
            charges = charges.filter(Request.project==opts.project)
        if opts.resource:
            charges = charges.filter(Request.resource==opts.resource)
        charges = (
            charge for charge in charges
            if charge.active
        )
        return charges
    
    else:
        if not opts.holds:
            raise Exception("must specify hold(s)")
        if opts.amount is None:
            raise Exception("must specify an amount")
        charges = Charge.distributed(
            holds = opts.holds,
            amount = opts.amount,
            comment = opts.comment,
        )
        clusterbank.model.Session.commit()
        clusterbank.model.Session.flush()
        return charges
