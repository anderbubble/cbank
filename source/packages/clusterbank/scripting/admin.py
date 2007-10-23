import sys
import os

import clusterbank
import clusterbank.model
from clusterbank import scripting
import clusterbank.scripting.options
from clusterbank.scripting import \
    MissingArgument, InvalidArgument, ExtraArguments


class OptionParser (scripting.OptionParser):
    
    standard_option_list = [
        scripting.options.user.having(help="change or list PERMISSIONS for USER"),
        scripting.options.grant.having(help="grant PERMISSIONS"),
        scripting.options.revoke.having(help="revoke PERMISSIONS"),
        scripting.options.list.having(help="list PERMISSIONS available for USER"),
    ]

    __defaults__ = dict(
        grant = None,
        revoke = None,
        list = False,
    )
    
    __version__ = clusterbank.__version__
    __usage__ = "%prog <user> [OPTIONS]"
    __description__ = "Grant or revoke PERMISSIONS for a user."


def run (argv=None):
    if argv is None:
        argv = sys.argv
    
    scripting.verify_configured()
    
    parser = OptionParser(prog=os.path.basename(argv[0]))
    options, args = parser.parse_args(args=argv[1:])
    arg_parser = scripting.ArgumentParser(args)
    
    user = arg_parser.get(scripting.options.user, options, arg="user")
    
    if options.grant:
        for permission in options.grant:
            setattr(user, "can_" + permission, True)
    if options.revoke:
        for permission in options.revoke:
            setattr(user, "can_" + permission, False)
    
    arg_parser.verify_empty()
    
    clusterbank.model.Session.flush()
    clusterbank.model.Session.commit()
    
    if options.list:
        permissions = (
            permission for permission in ("request", "allocate", "lien", "charge", "refund")
            if getattr(user, "can_" + permission)
        )
        return permissions
