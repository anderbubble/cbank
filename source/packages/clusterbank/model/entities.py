"""Cluster entity model.

Classes:
Resource -- resource that can be allocated
Project -- project to which resources can be allocated
"""

from datetime import datetime

from sqlalchemy import desc
import sqlalchemy.exceptions

import clusterbank
import clusterbank.exceptions as exceptions
from clusterbank.model.accounting import CreditLimit

__all__ = ["Project", "Resource"]


class Entity (object):
    
    def __repr__ (self):
        return "<%s %r>" % (self.__class__.__name__, self.id)
    
    def __str__ (self):
        if self.name is not None:
            return str(self.name)
        else:
            return "?"
    
    @classmethod
    def by_name (cls, name):
        """Get (or create) an entity based on its name upstream.
        
        Arguments:
        name -- upstream name of the entity
        """
        Upstream = getattr(clusterbank.upstream, cls.__name__)
        try:
            upstream_entity = Upstream.by_name(name)
        except clusterbank.upstream.NotFound:
            raise exceptions.NotFound("%s %r not found" % (cls.__name__.lower, name))
        try:
            return cls.query.filter(cls.id==upstream_entity.id).one()
        except sqlalchemy.exceptions.InvalidRequestError:
            return cls(id=upstream_entity.id)
    
    def _get_name (self):
        """Intelligent property accessor."""
        Upstream = getattr(clusterbank.upstream, self.__class__.__name__)
        upstream_entity = Upstream.by_id(self.id)
        return upstream_entity.name
    
    name = property(_get_name)


class Project (Entity):
    
    """Project to which resources can be allocated.
    
    Properties:
    id -- unique integer identifier
    name -- canonical name of the project (from upstream)
    requests -- request from the project
    allocations -- allocations to the project
    credit_limits -- credit limits for the project
    credit_limit -- credit limit for a resource at a given datetime
    
    Exceptions:
    DoesNotExist -- the specified project does not exist
    InsufficientFunds -- not enough funds to perform an action
    """
    
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
    
    def credit_limit (self, resource, datetime=datetime.now):
        try:
            datetime = datetime()
        except TypeError:
            pass
        credit_limits = CreditLimit.query.filter(CreditLimit.project==self)
        credit_limits = credit_limits.filter(CreditLimit.resource==resource)
        credit_limits = credit_limits = credit_limits.filter(CreditLimit.start<=datetime)
        credit_limits = credit_limits.order_by(desc("start"))
        try:
            return credit_limits[0]
        except IndexError:
            return None


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
