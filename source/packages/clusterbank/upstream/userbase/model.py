"""Userbase model for userbase plugin.

Classes:
Project -- upstream project
Resource -- upstream resource

Exceptions:
DoesNotExist -- requested entity does not exist
"""

from sqlalchemy import exceptions


__all__ = ["Project", "Resource"]


class DoesNotExist (Exception):
    """The specified entity does not exist."""
    
    def __init__ (self, identifier):
        self.identifier = identifier
    
    def __str__ (self):
        return "entity %r does not exist" % self.identifier


class UpstreamEntity (object):
    """Superclass for entities in the upstream model.
    
    Class methods:
    by_id -- Retrieve an entity by canonical identifier.
    by_name -- Retrieve an entity by name.
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
    
    def __repr__ (self):
        return "<%s %r>" % (self.__class__.__name__, self.id)
    
    def __str__ (self):
        if self.name is not None:
            return str(self.name)
        else:
            return "?"
    
    @classmethod
    def by_id (cls, id):
        """Retrieve an entity by identifier.
        
        Arguments:
        id -- Canonical, immutable, integer identifier.
        """
        try:
            entity = cls.query.filter_by(id=id).one()
        except exceptions.InvalidRequestError:
            raise cls.DoesNotExist(id)
        return entity
    
    @classmethod
    def by_name (cls, name):
        """Retrieve an entity by name.
        
        Arguments:
        name -- Canonical string identifier.
        """
        try:
            entity = cls.query.filter_by(name=name).one()
        except exceptions.InvalidRequestError:
            raise cls.DoesNotExist(name)
        return entity


class Project (UpstreamEntity):
    """Upstream project.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string id.
    
    Class methods:
    by_id -- Retrieve a project by identifier.
    by_name -- Retrieve a project by name.
    
    Exceptions:
    DoesNotExist -- The specified project does not exist.
    """
    
    class DoesNotExist (DoesNotExist):
        """The specified project does not exist."""
        
        def __str__ (self):
            return "project %r does not exist" % self.identifier


class Resource (UpstreamEntity):
    """Upstream resource.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string identifier.
    
    Class methods:
    by_id -- Retrieve a resource by identifier.
    by_name -- Retrieve a resource by name.
    
    Exceptions:
    DoesNotExist -- The specified resource does not exist.
    """
    
    class DoesNotExist (DoesNotExist):
        """The specified project does not exist."""
        
        def __str__ (self):
            return "resource %r does not exist" % self.identifier
