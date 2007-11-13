"""Cluster entity model.

Classes:
Resource -- resource that can be allocated
Project -- project to which resources can be allocated
"""

from sqlalchemy import exceptions

from clusterbank import upstream

__all__ = ["Project", "Resource"]


class Entity (object):
    
    def __repr__ (self):
        try:
            return "<%s %i>" % (self.__class__.__name__, self.id)
        except TypeError:
            return "<%s ?>" % self.__class__.__name__
    
    def __str__ (self):
        return self.name or "unknown"
    
    @classmethod
    def by_name (cls, name):
        """Get (or create) an entity based on its name upstream.
        
        Arguments:
        name -- upstream name of the entity
        """
        Upstream = getattr(upstream, cls.__name__)
        try:
            upstream_entity = Upstream.by_name(name)
        except Upstream.DoesNotExist:
            raise cls.DoesNotExist()
        try:
            return cls.query.filter(cls.id==upstream_entity.id).one()
        except exceptions.InvalidRequestError:
            return cls(id=upstream_entity.id)


class Project (Entity):
    
    """Project to which resources can be allocated.
    
    Properties:
    id -- unique integer identifier
    name -- canonical name of the project (from upstream)
    requests -- request from the project
    allocations -- allocations to the project
    credit_limits -- credit limits for the project
    
    Exceptions:
    DoesNotExist -- the specified project does not exist
    InsufficientFunds -- not enough funds to perform an action
    """
    
    class DoesNotExist (Exception):
        """The specified project does not exist."""
    
    class InsufficientFunds (Exception):
        """Not enough funds to perform an action."""
    
    def __init__ (self, **kwargs):
        """Initialize a project.
        
        Keyword arguments:
        id -- unique integer identifier
        requests -- requests from the project
        allocations -- allocations to the project
        credit_limits -- credit limits for the project
        """
        self.id = kwargs.get("id")
        self.requests = kwargs.get("requests", [])
        self.allocations = kwargs.get("allocations", [])
        self.credit_limits = kwargs.get("credit_limits", [])
    
    def _get_name (self):
        """Intelligent property accessor."""
        upstream_project = upstream.Project.by_id(self.id)
        return upstream_project.name
    
    name = property(_get_name)


class Resource (Entity):
    
    """Resource that can be allocated to a project.
    
    Properties:
    id -- canonical id of the resource
    name -- canonical name of the resource (from upstream)
    requests -- requests for the resource
    allocations -- allocations of the resource
    credit_limits -- credit limits on the resource
    
    Exceptions:
    DoesNotExist -- the specified resource does not exist
    """
    
    class DoesNotExist (Exception):
        """The specified resource does not exist."""
    
    def __init__ (self, **kwargs):
        """Initialize a resource.
        
        Keyword arguments:
        id -- canonical id of the resource
        requests -- requests for the resource
        credit_limits -- credit limits on the resource
        """
        self.id = kwargs.get("id")
        self.requests = kwargs.get("requests", [])
        self.allocations = kwargs.get("allocations", [])
        self.credit_limits = kwargs.get("credit_limits", [])
    
    def _get_name (self):
        """Intelligent property accessor."""
        upstream_resource = upstream.Resource.by_id(self.id)
        return upstream_resource.name
    
    name = property(_get_name)
