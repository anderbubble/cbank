"""Plugins for user/project/resource tracking are defined here.

Plugins should make the plugin interface available at the top level (whether
that be a module or package).

class Project:
    
    id = int
    name = str
    
    @classmethod
    def by_id (cls, id): --> Resource | DoesNotExist
    
    @classmethod
    def by_name (cls, name): --> Resource | DoesNotExist


class Resource:
    
    id = int
    name = str
    
    @classmethod
    def by_id (cls, id): --> Resource | DoesNotExist
    
    @classmethod
    def by_name (cls, name): --> Resource | DoesNotExist

class DoesNotExist (Exception)
"""

import warnings
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError

__all__ = ["userbase"]

config = SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    upstream_module_name = config.get("upstream", "module")
except (NoSectionError, NoOptionError):
    warnings.warn("no upstream module specified", UserWarning)
else:
    try:
        base_name = upstream_module_name.split(".")[0]
        upstream_module = __import__(upstream_module_name, locals(), globals(), ["Project", "Resource"])
    except ImportError:
        raise
        warnings.warn("invalid upstream module: %s" % (upstream_module_name), UserWarning)
    else:
        Project = upstream_module.Project
        Resource = upstream_module.Resource
        __all__.extend(["Project", "Resource"])
