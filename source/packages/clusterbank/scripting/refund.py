import sys
import os
from optparse import OptionParser

import clusterbank
import clusterbank.model
from clusterbank.model import Request, Charge, Refund
from clusterbank.scripting import options, verify_configured


parser = OptionParser(
    version = clusterbank.__version__,
    usage = os.linesep.join(["",
        "    %prog <charge> <amount> [options]",
        "    %prog --list [options]",
    ]),
    description = "Refund amount previously charged against a project on a resource.",
)
parser.add_option(options.list.having(help="list active refunds"))
parser.add_option(options.project.having(help="list refunds for PROJECT"))
parser.add_option(options.resource.having(help="list refunds for RESOURCE"))
parser.add_option(options.hold.having(help="list refunds under LIEN"))
parser.add_option(options.charge.having(help="post or list refunds of CHARGE"))
parser.add_option(options.amount.having(help="refund AMOUNT"))
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
        refunds = Refund.query()
        if opts.charge:
            refunds = refunds.filter(Refund._charge==opts.charge)
        refunds = refunds.join("charge")
        if opts.hold:
            refunds = refunds.filter(Charge.hold==opts.hold)
        refunds = refunds.join(["charge", "hold", "allocation", "request"])
        if opts.project:
            refunds = refunds.filter(Request.project==opts.project)
        if opts.resource:
            refunds = refunds.filter(Request.resource==opts.resource)
        refunds = (
            refund for refund in refunds
            if refund.active
        )
        return refunds
    
    else:
        if not opts.charge:
            raise Exception("must specify a charge to refund")
        if opts.amount is None:
            raise Exception("must specify an amount")
        refund = Refund(
            charge = opts.charge,
            amount = opts.amount,
            comment = opts.comment,
        )
        clusterbank.model.Session.commit()
        return [refund]
