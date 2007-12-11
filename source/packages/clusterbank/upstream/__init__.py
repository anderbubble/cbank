"""Plugins for user/project/resource tracking are defined here.

Plugins should make the plugin interface available at the top level (whether
that be a module or package).

class Project (object):
    
    id = int()
    name = str()
    
    @classmethod
    def by_id (cls, id): pass # return Project() or raise DoesNotExist()
    
    @classmethod
    def by_name (cls, name): pass # return Project() or raise DoesNotExist()


class Resource (object):
    
    id = int()
    name = str()
    
    @classmethod
    def by_id (cls, id): pass # return Resource() or raise DoesNotExist()
    
    @classmethod
    def by_name (cls, name): pass # return Resource() or raise DoesNotExist()

class DoesNotExist (Exception): pass

Packages:
userbase -- uses the MCS userbase

Project and Resource classes are imported into this module as directed by
a config file.

Configuration:
/etc/clusterbank.conf -- [upstream] module
"""

import warnings
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError

__all__ = []

config = SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    upstream_module_name = config.get("upstream", "module")
except (NoSectionError, NoOptionError):
    warnings.warn("no upstream module specified", UserWarning)
else:
    try:
        upstream_module = __import__(upstream_module_name, locals(), globals(), ["Project", "Resource"])
    except ImportError:
        warnings.warn("invalid upstream module: %s" % (upstream_module_name), UserWarning)
    else:
        Project = upstream_module.Project
        Resource = upstream_module.Resource
        __all__.extend(["Project", "Resource"])
