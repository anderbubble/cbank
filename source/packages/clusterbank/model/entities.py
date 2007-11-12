"""Cluster entity model.

Classes:
Resource -- a resource (usually for computation)
Project -- a project in the system
"""

from datetime import datetime

from sqlalchemy import exceptions, desc

from clusterbank import upstream
from clusterbank.model.accounting import \
    Request, Allocation, CreditLimit, Hold, Charge, Refund

__all__ = [
    "Project", "Resource",
]


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
    
    """A logical project.
    
    Exceptions:
    DoesNotExist -- the specified project does not exist
    InsufficientFunds -- not enough funds to perform an action
    
    Properties:
    id -- canonical id of the project
    name -- canonical name of the project (from upstream)
    credit_limits -- available credit per-resource
    requests -- requests made for the project
    
    Methods:
    amount_allocated -- sum of amount allocated to a resource
    amount_held -- sum of amount committed to uncharged holds
    amount_charged -- sum of effective charges
    amount_used -- sum of amount held and amount charged
    amount_available -- difference of amount allocated and amount used
    credit_limit -- current credit limit (value) for the resource
    credit_used -- negative amount used
    credit_available -- difference of credit limit and credit used
    """
    
    class DoesNotExist (Exception):
        """The specified project does not exist."""
    
    class InsufficientFunds (Exception):
        """Not enough funds to perform an action."""
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.requests = kwargs.get("requests", [])
        self.allocations = kwargs.get("allocations", [])
        self.credit_limits = kwargs.get("credit_limits", [])
        self.holds = kwargs.get("holds", [])
        self.charges = kwargs.get("charges", [])
    
    def _get_name (self):
        """Return the name of the upstream project."""
        upstream_project = upstream.Project.by_id(self.id)
        return upstream_project.name
    
    name = property(_get_name)


class Resource (Entity):
    
    """A logical resource.
    
    Properties:
    id -- canonical id of the resource
    name -- canonical name of the resource (from upstream)
    credit_limits -- credit limits posted for the resource
    requests -- requests posted for the resource
    
    Exceptions:
    DoesNotExist -- The specified resource does not exist.
    """
    
    class DoesNotExist (Exception):
        """The specified resource does not exist."""
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.requests = kwargs.get("requests", [])
        self.allocations = kwargs.get("allocations", [])
        self.credit_limits = kwargs.get("credit_limits", [])
        self.holds = kwargs.get("holds", [])
        self.charges = kwargs.get("charges", [])
    
    def _get_name (self):
        """Return the name of the upstream resource."""
        upstream_resource = upstream.Resource.by_id(self.id)
        return upstream_resource.name
    
    name = property(_get_name)
