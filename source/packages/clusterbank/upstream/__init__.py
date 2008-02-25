"""Plugins for user/project/resource tracking are defined here.

Plugins should make the plugin interface available at the top level (whether
that be a module or package).

class Project (object):
    
    id = int()
    name = str()
    
    @classmethod
    def by_id (cls, id): pass # return Project() or raise NotFound()
    
    @classmethod
    def by_name (cls, name): pass # return Project() or raise NotFound()


class Resource (object):
    
    id = int()
    name = str()
    
    @classmethod
    def by_id (cls, id): pass # return Resource() or raise NotFound()
    
    @classmethod
    def by_name (cls, name): pass # return Resource() or raise NotFound()

class DoesNotExist (Exception): pass

Packages:
userbase -- uses the MCS userbase

Project and Resource classes are imported into this module as directed by
a config file.

Configuration:
/etc/clusterbank.conf -- [upstream] module
"""

__all__ = []
