"""clusterbank

A library for managing and accounting for the use of resources.

Packages:
model -- data model
scripting -- interface layers
"""

import warnings
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError

__all__ = ["model", "interfaces", "upstream"]

__version__ = "trunk"

class UpstreamProxy (object):
    
    def __init__ (self, use=None):
        self.use = use
    
    def __getattr__ (self, name):
        return getattr(self.use, name)

upstream = UpstreamProxy()

config = SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    upstream_module_name = config.get("upstream", "module")
except (NoSectionError, NoOptionError):
    upstream_module_name = "clusterbank.upstreams.default"
try:
    upstream.use = __import__(upstream_module_name, locals(), globals(), ["get_project_name", "get_project_id", "get_resource_name", "get_resource_id"])
except ImportError:
    warnings.warn("invalid upstream module: %s" % (upstream_module_name), UserWarning)
