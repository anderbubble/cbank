"""Userbase model for userbase plugin.

Classes:
UpstreamEntity -- base class for upstream entities
User -- upstream user
Project -- upstream project
Resource -- upstream resource
"""

from sqlalchemy import exceptions


__all__ = ["User", "Project", "Resource"]


class UpstreamEntity (object):
    """Superclass for entities in the upstream model.
    
    Class methods:
    by_id -- Retrieve an entity by canonical identifier.
    by_name -- Retrieve an entity by name.
    
    Exceptions:
    DoesNotExist -- The specified entity does not exist.
    """
    
    
    class DoesNotExist (Exception):
        """The specified entity does not exist."""
        
        label = "entity"
        
        def __str__ (self):
            try:
                return "%s %i does not exist" % (self.label, self.message)
            except TypeError:
                return '%s "%s" does not exist' % (self.label, self.message)
    
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def __str__ (self):
        return self.name or "?"
    
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


class User (UpstreamEntity):
    """Upstream user.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string identifier.
    projects -- Projects the user is a member of.
    
    Class methods:
    by_id -- Retrieve a user by identifier.
    by_name -- Retrieve a user by name.
    
    Exceptions:
    DoesNotExist -- The specified user does not exist.
    """
    
    class DoesNotExist (UpstreamEntity.DoesNotExist):
        """The specified user does not exist."""
        
        label = "user"


class Project (UpstreamEntity):
    """Upstream project.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string id.
    users -- Users that are members of the project.
    
    Class methods:
    by_id -- Retrieve a project by identifier.
    by_name -- Retrieve a project by name.
    
    Exceptions:
    DoesNotExist -- The specified project does not exist.
    """
    
    class DoesNotExist (UpstreamEntity.DoesNotExist):
        """The specified project does not exist."""
        
        label = "project"


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
    
    class DoesNotExist (UpstreamEntity.DoesNotExist):
        """The specified project does not exist."""
        
        label = "resource"
