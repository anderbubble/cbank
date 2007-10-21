"""Plugins for user/project/resource tracking are defined here.

Plugins should make the plugin interface available at the top level (whether
that be a module or package).

class User:
    
    id = int
    name = str
    projects = iterable <Project>
    
    @classmethod
    def by_id (cls, id): --> Resource | DoesNotExist
    
    @classmethod
    def by_name (cls, name): --> Resource | DoesNotExist


class Project:
    
    id = int
    name = str
    users = iterable <User>
    
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
config.read(["/etc/clusterbank.conf", "clusterbank.conf"])

try:
    upstream_module_name = config.get("main", "upstream")
except (NoSectionError, NoOptionError):
    warnings.warn("no upstream module specified", ImportWarning)
else:
    try:
        base_name = upstream_module_name.split(".")[0]
        upstream_module = __import__(upstream_module_name, locals(), globals(), ["*"])
    except ImportError:
        raise
        warnings.warn("invalid upstream module: %s" % (upstream_module_name), ImportWarning)
    else:
        User = upstream_module.User
        Project = upstream_module.Project
        Resource = upstream_module.Resource
        __all__.extend(["User", "Project", "Resource"])
