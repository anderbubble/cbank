import sys
import os

import elixir

import clusterbank
from clusterbank import scripting
from clusterbank.scripting import \
    MissingArgument, InvalidArgument, ExtraArguments


class OptionParser (scripting.OptionParser):
    
    standard_option_list = [
        scripting.OPTIONS['user'].having(help="change or list PERMISSIONS for USER"),
        scripting.OPTIONS['grant'].having(help="grant PERMISSIONS"),
        scripting.OPTIONS['revoke'].having(help="revoke PERMISSIONS"),
        scripting.OPTIONS['list'].having(help="list PERMISSIONS available for USER"),
    ]

    __defaults__ = dict(
        grant = None,
        revoke = None,
        list = False,
    )
    
    __version__ = clusterbank.__version__
    __usage__ = "%prog <user> [OPTIONS]"
    __description__ = "Grant or revoke PERMISSIONS for a user."


def run (argv=sys.argv):
    parser = OptionParser(prog=os.path.basename(argv[0]))
    options, args = parser.parse_args(args=argv[1:])
    arg_parser = scripting.ArgumentParser(args)
    
    try:
        user = arg_parser.get(scripting.OPTIONS['user'], options)
    except arg_parser.NoValue, e:
        raise MissingArgument("%s: user" % e)
    except arg_parser.InvalidArgument, e:
        raise InvalidArgument("%s: (user not found)" % e)
    
    if options.grant:
        for permission in options.grant:
            setattr(user, "can_" + permission, True)
    if options.revoke:
        for permission in options.revoke:
            setattr(user, "can_" + permission, False)
    
    try:
        arg_parser.verify_empty()
    except arg_parser.NotEmpty, e:
        raise ExtraArguments(e)
    
    elixir.objectstore.flush()
    
    if options.list:
        permissions = (
            permission for permission in scripting.PERMISSIONS
            if getattr(user, "can_" + permission)
        )
        return permissions
