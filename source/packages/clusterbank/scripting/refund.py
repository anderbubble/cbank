import sys
import os

import clusterbank
import clusterbank.model
from clusterbank import scripting
from clusterbank.model import Request, Refund
from clusterbank.scripting import \
    ScriptingError, NotPermitted, \
    MissingArgument, InvalidArgument, ExtraArguments


class OptionParser (scripting.OptionParser):
    
    standard_option_list = [
        scripting.OPTIONS['list'].having(help="list active refunds"),
        scripting.OPTIONS['user'].having(help="post refund as USER"),
        scripting.OPTIONS['project'].having(help="list refunds for PROJECT"),
        scripting.OPTIONS['resource'].having(help="list refunds for RESOURCE"),
        scripting.OPTIONS['lien'].having(help="list refunds under LIEN"),
        scripting.OPTIONS['charge'].having(help="post or list refunds of CHARGE"),
        scripting.OPTIONS['time'].having(help="refund TIME"),
        scripting.OPTIONS['comment'].having(help="misc. NOTES"),
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
        
        # At this point, no more arguments are used.
        try:
            arg_parser.verify_empty()
        except arg_parser.NotEmpty, e:
            raise ExtraArguments(e)
        
        refunds = Refund.query()
        
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
        
        try:
            charge = arg_parser.get(scripting.OPTIONS['charge'], options)
        except arg_parser.NoValue, e:
            raise MissingArgument("%s: charge" % e)
        except arg_parser.InvalidArgument, e:
            raise InvalidArgument("%s (charge not found)" % e)
        try:
            time = arg_parser.get(scripting.OPTIONS['time'], options)
        except arg_parser.NoValue, e:
            raise MissingArgument("%s: time" % e)
        except arg_parser.InvalidArgument, e:
            raise InvalidArgument(e)
        
        kwargs = dict(
            charge = charge,
            time = time,
        )
        
        # At this point, no more arguments are used.
        try:
            arg_parser.verify_empty()
        except arg_parser.NotEmpty, e:
            raise ExtraArguments(e)
        
        if options.comment is not None:
            kwargs['comment'] = options.comment
        
        try:
            refund = user.refund(**kwargs)
        except (user.NotPermitted, charge.ExcessiveRefund), e:
            raise NotPermitted(e)
        except ValueError, e:
            raise InvalidArgument(e)
        
        clusterbank.model.Session.flush()
        clusterbank.model.Session.commit()
        
        return [refund]
