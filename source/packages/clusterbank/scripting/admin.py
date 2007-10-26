import sys
import os
from optparse import OptionParser

import clusterbank
import clusterbank.model
from clusterbank.scripting import options, verify_configured, \
    ArgumentParser, MissingArgument, InvalidArgument, ExtraArguments

parser = OptionParser(
    version = clusterbank.__version__,
    usage = "%prog <user> [OPTIONS]",
    description = "Grant or revoke PERMISSIONS for a user."
)
parser.add_option(options.user.having(help="change or list PERMISSIONS for USER"))
parser.add_option(options.grant.having(help="grant PERMISSIONS"))
parser.add_option(options.revoke.having(help="revoke PERMISSIONS"))
parser.add_option(options.list.having(help="list PERMISSIONS available for USER"))
parser.set_defaults(grant=None, revoke=None, list=False)

def run (argv=None):
    if argv is None:
        argv = sys.argv
    
    verify_configured()
    parser.prog = os.path.basename(argv[0])
    opts, args = parser.parse_args(args=argv[1:])
    arg_parser = ArgumentParser(args)
    
    user = arg_parser.get(options.user, opts, arg="user")
    
    if opts.grant:
        for permission in opts.grant:
            setattr(user, "can_" + permission, True)
    if opts.revoke:
        for permission in opts.revoke:
            setattr(user, "can_" + permission, False)
    
    arg_parser.verify_empty()
    
    clusterbank.model.Session.flush()
    clusterbank.model.Session.commit()
    
    if opts.list:
        permissions = (
            permission for permission in ("request", "allocate", "lien", "charge", "refund")
            if getattr(user, "can_" + permission)
        )
        return permissions
