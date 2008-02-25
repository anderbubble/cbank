"""clusterbank

A library for managing and accounting for the use of resources.

Packages:
model -- data model
scripting -- interface layers
upstream -- pluggable upstream module (reflected in the model)
"""

import warnings
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError

import clusterbank.upstream

__all__ = ["model", "interfaces", "upstream"]

__version__ = "trunk"

upstream = None

config = SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    upstream_module_name = config.get("upstream", "module")
except (NoSectionError, NoOptionError):
    warnings.warn("no upstream module specified", UserWarning)
else:
    try:
        upstream = __import__(upstream_module_name, locals(), globals(), ["Project", "Resource"])
    except ImportError:
        warnings.warn("invalid upstream module: %s" % (upstream_module_name), UserWarning)
