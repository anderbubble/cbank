from clusterbank.scripting.base import Option

__all__ = [
    'allocation', 'charge', 'comment', 'credit', 'expiration',
    'grant', 'hold', 'holds', 'list', 'project', 'request',
    'resource', 'revoke', 'start', 'amount',
]

allocation = Option("-a", "--allocation",
    dest = "allocation",
    type = "allocation",
    action = "store",
    metavar="ALLOCATION",
)

grant = Option("-g", "--grant",
    dest = "grant",
    type = "permissions",
    action = "store",
    metavar = "PERMISSIONS",
)

revoke = Option("-r", "--revoke",
    dest = "revoke",
    type = "permissions",
    action = "store",
    metavar="PERMISSIONS",
)

list = Option("-l", "--list",
    dest = "list",
    action = "store_true",
)

hold = Option("-n", "--hold",
    dest = "hold",
    type = "hold",
    action = "store",
    metavar = "LIEN",
)

holds = Option("-n", "--holds",
    dest = "holds",
    type = "holds",
    action = "store",
    metavar="LIENS",
)

project = Option("-p", "--project",
    dest = "project",
    type = "project",
    action = "store",
    metavar = "PROJECT",
)

resource = Option("-r", "--resource",
    dest = "resource",
    type = "resource",
    action = "store",
    metavar = "RESOURCE",
)

request = Option("-q", "--request",
    dest = "request",
    type = "request",
    action = "store",
    metavar = "REQUEST",
)

amount = Option("-t", "--amount",
    dest = "amount",
    type = "int",
    action = "store",
    metavar = "AMOUNT",
)

credit = Option("-c", "--credit-limit",
    dest = "credit",
    type = "int",
    action = "store",
    metavar = "LIMIT",
)

start = Option("-s", "--start",
    dest = "start",
    type = "date",
    action = "store",
    metavar = "DATE",
)

expiration = Option("-x", "--expiration",
    dest = "expiration",
    type = "date",
    action = "store",
    metavar = "DATE",
)

comment = Option("-m", "--comment",
   dest = "comment",
   type = "string",
   action = "store",
   metavar = "NOTES",
)

charge = Option("-c", "--charge",
    dest = "charge",
    type = "charge",
    action = "store",
    metavar = "CHARGE",
)
