import warnings
from ConfigParser import SafeConfigParser

import sqlalchemy
from sqlalchemy import create_engine
import elixir

import upstream

__all__ = ["models", "scripting", "settings", "statements", "upstream"]

__version__ = "0.2.x"


config = SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    uri = config.get("main", "database")
except:
    warnings.warn("no database specified")
else:
    try:
        elixir.metadata.bind = create_engine(uri)
    except:
        warnings.warn("invalid database: %s" % uri)

try:
    upstream_type = config.get("main", "upstream")
except:
    warnings.warn("no upstream type specified")
else:
    if upstream_type == "userbase":
        from upstream import userbase
        upstream.User = userbase.User
        upstream.Project = userbase.Project
        upstream.Resource = userbase.Resource
    else:
        warnings.warn("invalid upstream type: %s" % upstream_type)
