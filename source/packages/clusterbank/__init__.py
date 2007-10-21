import warnings
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError

import sqlalchemy
from sqlalchemy import create_engine

import clusterbank.model
import clusterbank.upstream

__all__ = ["model", "scripting", "upstream"]

__version__ = "0.2.x"

def get_end_module (name):
    mod = __import__(name)
    components = name.split(".")
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

config = SafeConfigParser()
config.read(["/etc/clusterbank.conf", "clusterbank.conf"])

try:
    uri = config.get("main", "database")
except (NoSectionError, NoOptionError):
    warnings.warn("no database specified", ImportWarning)
else:
    try:
        clusterbank.model.metadata.bind = create_engine(uri)
    except Exception, e:
        warnings.warn("invalid database: %s (%s)" % (uri, e), ImportWarning)

try:
    upstream_module_name = config.get("main", "upstream")
except (NoSectionError, NoOptionError):
    warnings.warn("no upstream module specified", ImportWarning)
else:
    try:
        upstream_module = get_end_module(upstream_module_name)
    except ImportError:
        warnings.warn("invalid upstream module: %s" % (upstream_module_name), ImportWarning)
    else:
        clusterbank.upstream.User = upstream_module.User
        clusterbank.upstream.Project = upstream_module.Project
        clusterbank.upstream.Resource = upstream_module.Resource
