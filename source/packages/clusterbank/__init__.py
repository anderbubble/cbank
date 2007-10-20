import warnings
from ConfigParser import SafeConfigParser

import sqlalchemy
from sqlalchemy import create_engine

import upstream

__all__ = ["models", "scripting", "settings", "statements", "upstream"]

__version__ = "0.2.x"

def get_end_module (name):
    mod = __import__(name)
    components = name.split(".")
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

config = SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    uri = config.get("main", "database")
except:
    warnings.warn("no database specified", ImportWarning)
else:
    try:
        clusterbank.model.metadata.metadata.bind = create_engine(uri)
    except:
        warnings.warn("invalid database: %s" % (uri), ImportWarning)

try:
    upstream_module_name = config.get("main", "upstream")
except:
    warnings.warn("no upstream module specified", ImportWarning)
else:
    try:
        upstream_module = get_end_module(upstream_module_name)
    except ImportError:
        warnings.warn("invalid upstream module: %s" % (upstream_module_name), ImportWarning)
    else:
        upstream.User = upstream_module.User
        upstream.Project = upstream_module.Project
        upstream.Resource = upstream_module.Resource
