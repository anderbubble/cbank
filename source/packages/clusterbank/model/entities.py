"""Cluster entity model.

Classes:
User -- a user in the system
Resource -- a resource (usually for computation)
Project -- a project in the system
"""

from datetime import datetime

from sqlalchemy import exceptions, desc

from clusterbank import upstream
from clusterbank.model.accounting import \
    Request, Allocation, CreditLimit, Lien, Charge, Refund

__all__ = [
    "User", "Project", "Resource",
]

class User (object):
    
    """A logical user.
    
    Relationships:
    credit_limits -- CreditLimits posted by the user.
    requests -- Requests made by the user.
    allocations -- Allocations made by the user.
    liens -- Liens made by the user.
    charges -- Charges made by the user.
    refunds -- Refunds made by the user.
    unit_factors -- Factors set by the user.
    
    Attributes:
    id -- Canonical id of the user.
    can_request -- Permission to make requests.
    can_allocate -- Permission to allocate time or credit.
    can_lien -- Permission to post liens.
    can_charge -- Permission to charge liens.
    can_refund -- Permission to refund charges.
    
    Properties:
    name -- Human-readable name from upstream.
    projects -- Result set of local projects by upstream membership.
    
    Methods:
    member_of -- A user is a member of a project.
    request -- Request time on a resource for a project.
    allocate -- Allocate time for a request.
    allocate_credit -- Allocate a credit limit for a project.
    lien -- Acquire a lien against an allocation.
    charge -- Charge a lien.
    refund -- Refund a charge.
    
    Exceptions:
    DoesNotExist -- The specified user does not exist.
    NotPermitted -- An intentional denial of an action.
    """
    
    class DoesNotExist (Exception):
        """The specified user does not exist."""
    
    class NotPermitted (Exception):
        """An intentional denail of an action."""
    
    @classmethod
    def by_name (cls, name):
        """Get (or create) a user based on its name upstream.
        
        Arguments:
        name -- The upstream name of the user.
        """
        try:
            upstream_user = upstream.User.by_name(name)
        except upstream.User.DoesNotExist:
            raise cls.DoesNotExist()
        try:
            return cls.query.filter_by(id=upstream_user.id).one()
        except exceptions.InvalidRequestError:
            return cls(id=upstream_user.id)
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.can_request = kwargs.get("can_request", False)
        self.can_allocate = kwargs.get("can_allocate", False)
        self.can_lien = kwargs.get("can_lien", False)
        self.can_charge = kwargs.get("can_charge", False)
        self.can_refund = kwargs.get("can_refund", False)
        self.credit_limits = kwargs.get("credit_limits", [])
        self.requests = kwargs.get("requests", [])
        self.allocations = kwargs.get("allocations", [])
        self.liens = kwargs.get("liens", [])
        self.charges = kwargs.get("charges", [])
        self.refunds = kwargs.get("refunds", [])
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def __str__ (self):
        return self.name
    
    def _get_name (self):
        """Return the name of the upstream user."""
        upstream_user = upstream.User.by_id(self.id)
        return upstream_user.name
    name = property(_get_name)
    
    def _get_projects (self):
        """Return the set of projects that this user is a member of."""
        upstream_projects = upstream.User.by_id(self.id).projects
        local_projects = [
            Project.by_name(name)
            for name in (project.name for project in upstream_projects)
        ]
        return local_projects
    projects = property(_get_projects)
    
    def member_of (self, project):
        """Whether or not a user is a member of a given project."""
        upstream_user = upstream.User.by_id(self.id)
        upstream_project = upstream.Project.by_id(project.id)
        return upstream_project in upstream_user.projects
    
    def request (self, **kwargs):
        """Request time on a resource."""
        return Request(poster=self, **kwargs)
    
    def allocate (self, **kwargs):
        """Allocate time on a resource in response to a request.
        
        Arguments:
        request -- Which request to allocate time for. (required)
        """
        return Allocation(poster=self, **kwargs)
    
    def allocate_credit (self, **kwargs):
        """Post a credit limit for a project.
        
        Arguments:
        project -- Which project to give credit to. (required)
        """
        return CreditLimit(poster=self, **kwargs)
    
    def lien (self, **kwargs):
        
        """Enter a lien against an allocation."""
        
        if kwargs.get("allocation") is not None:
            kwargs.pop('project', None)
            kwargs.pop('resource', None)
            lien = Lien(poster=self, **kwargs)
            return lien
        
        kwargs.pop("allocation", None)
        
        project = kwargs.pop('project')
        resource = kwargs.pop('resource')
        time = kwargs.pop('time')
        allocations = Allocation.query.join("request").filter_by(project=project, resource=resource)
        allocations = allocations.order_by([Allocation.c.expiration, Allocation.c.datetime])
        allocations = (
            allocation for allocation in allocations
            if allocation.active
        )
        
        # Post a lien to each allocation until all time has been liened.
        liens = list()
        allocation = None
        for allocation in allocations:
            # If the remaining time will fit into the allocation,
            # post a lien for it. Otherwise, post a lien for whatever
            # The allocation will support.
            if allocation.time_available >= time:
                lien = self.lien(allocation=allocation, time=time, **kwargs)
            else:
                lien = self.lien(allocation=allocation, time=allocation.time_available, **kwargs)
            liens.append(lien)
            time -= lien.time
            if time <= 0:
                break
        
        # If there is still time to be liened, add it to the last lien.
        # This allows liens to be posted that put the project negative.
        if time > 0:
            try:
                liens[-1].time += time
            except IndexError:
                # No lien has yet been created. Post a lien on the last
                # Allocation used.
                if allocation is None:
                    raise self.NotPermitted("There are no active allocations for %s on %s." % (project, resource))
                lien = self.lien(allocation=allocation, time=time, **kwargs)
                liens.append(lien)
        return liens
    
    def charge (self, **kwargs):
        
        """Charge time against a lien.
        
        Arguments:
        lien -- lien to charge against.
        liens -- liens to which the charge can be charged.
        """
        
        # If the charge is for a specific lien, post the charge.
        if kwargs.get("lien") is not None:
            kwargs.pop("liens", None)
            return Charge(poster=self, **kwargs)
        
        kwargs.pop("lien", None)
        
        # No specific lien has been given. Post a charge to each lien
        # in the pool until all time has been charged.
        charges = list()
        time = kwargs.pop("time")
        for lien in kwargs.pop("liens"):
            # If the remaining time will fit into the lien, post a
            # Charge for all of it. Otherwise, post a charge for what
            # the lien can support.
            if lien.time_available >= time:
                charge = self.charge(lien=lien, time=time, **kwargs)
            else:
                charge = self.charge(lien=lien, time=lien.time_available, **kwargs)
            charges.append(charge)
            time -= charge.time
            # Iterate through all liens to charge 0 on unused liens.
            # Charging 0 marks the lien as closed, and frees the liened
            # time.
            #if time <= 0:
            #    break
        
        # If there is time remaining, add it to the last charge.
        if time > 0:
            try:
                charges[-1].time += time
            except IndexError:
                # No charges have yet been made. Charge the last lien.
                try:
                    charge = self.charge(lien=lien, time=time, **kwargs)
                except NameError:
                    # There was no lien.
                    raise self.NotPermitted("No liens are available to be charged.")
                charges.append(charge)
        return charges
    
    def refund (self, **kwargs):
        """Refund time from a charge.
        
        Arguments:
        charge -- Which charge to refund. (required)
        """
        return Refund(poster=self, **kwargs)


class Project (object):
    
    """A logical project.
    
    Relationships:
    credit_limits -- Available credit per-resource.
    requests -- Requests made for the project.
    
    Exceptions:
    DoesNotExist -- The specified project does not exist.
    InsufficientFunds -- Not enough funds to perform an action.
    
    Attributes:
    id -- Canonical id of the project.
    
    Properties:
    name -- The upstream project name.
    users -- The users that are members of the project from upstream.
    allocations -- All allocations related to this project.
    charges -- All charges related to this project.
    liens -- All liens related to this project.
    
    Methods:
    has_member -- The group has a member.
    time_allocated -- Sum of time allocated to a resource.
    time_liened -- Sum of time committed to uncharged liens.
    time_charged -- Sum of effective charges.
    time_used -- Sum of time liened and time charged.
    time_available -- Difference of time allocated and time used.
    credit_limit -- Current credit limit for the resource.
    credit_used -- Negative time used.
    credit_available -- Difference of credit limit and credit used.
    """
    
    class DoesNotExist (Exception):
        """The specified project does not exist."""
    
    class InsufficientFunds (Exception):
        """Not enough funds to perform an action."""
    
    @classmethod
    def by_name (cls, name):
        """Get (or create) a project based on its upstream name.
        
        Arguments:
        name -- The upstream name of the project.
        """
        try:
            upstream_project = upstream.Project.by_name(name)
        except upstream.Project.DoesNotExist:
            raise cls.DoesNotExist()
        try:
            return cls.query.filter_by(id=upstream_project.id).one()
        except exceptions.InvalidRequestError:
            return cls(id=upstream_project.id)
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.credit_limits = kwargs.get("credit_limits", [])
        self.requests = kwargs.get("requests", [])
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def __str__ (self):
        return self.name
    
    def _get_name (self):
        """Return the name of the upstream project."""
        upstream_project = upstream.Project.by_id(self.id)
        return upstream_project.name
    name = property(_get_name)
    
    def _get_users (self):
        """Return the set of users who are members of this project."""
        upstream_users = upstream.Project.by_id(self.id).users
        local_users = [
            User.by_name(name)
            for name in (user.name for user in upstream_users)
        ]
        return local_users
    users = property(_get_users)
    
    def has_member (self, user):
        """Whether or not a given user is a member of a project."""
        upstream_project = upstream.Project.by_id(self.id)
        upstream_user = upstream.User.by_id(user.id)
        return upstream_user in upstream_project.users
    
    def _get_allocations (self):
        """Return the set of allocations for this project."""
        return Allocation.query.join("request").filter_by(project=self)
    allocations = property(_get_allocations)
    
    def _get_liens (self):
        """Return the set of liens posted against this project."""
        return Lien.query.join("request").filter_by(project=self)
    liens = property(_get_liens)
    
    def _get_charges (self):
        """Return the set of charges posted against this project."""
        return Charge.query.join("request").filter_by(project=self)
    charges = property(_get_charges)
    
    def time_allocated (self, resource):
        """Sum of time in active allocations."""
        allocations = Allocation.query.join("request").filter_by(
            project = self,
            resource = resource,
        )
        allocations = (
            allocation for allocation in allocations
            if allocation.active
        )
        time_allocated = 0
        for allocation in allocations:
            time_allocated += allocation.time
        return time_allocated
    
    def time_liened (self, resource):
        """Sum of time in active and open liens."""
        liens = Lien.query.join(["allocation", "request"]).filter_by(
            project = self,
            resource = resource,
        )
        liens = (
            lien for lien in liens
            if lien.active and lien.open
        )
        time_liened = 0
        for lien in liens:
            time_liened += lien.time
        return time_liened
    
    def time_charged (self, resource):
        """Sum of time in active charges."""
        charges = Charge.query.join(["lien", "allocation", "request"]).filter_by(
            project = self,
            resource = resource,
        )
        charges = (
            charge for charge in charges
            if charge.active
        )
        time_charged = 0
        for charge in charges:
            time_charged += charge.effective_charge
        return time_charged
    
    def time_used (self, resource):
        """Sum of time committed to liens and charges."""
        return self.time_liened(resource) \
            + self.time_charged(resource)
    
    def time_available (self, resource):
        """Difference of time allocated and time used."""
        return self.time_allocated(resource) \
            - self.time_used(resource)
    
    def credit_limit (self, resource):
        """The effective credit limit for a resource at a given date.
        
        Arguments:
        resource -- The applicable resource.
        """
        credit_limits = CreditLimit.query.filter(
            CreditLimit.c.start <= datetime.now()
        ).filter_by(project=self, resource=resource).order_by(desc(CreditLimit.c.start))
        try:
            return credit_limits[0].time
        except IndexError:
            return 0
    
    def credit_used (self, resource):
        delta = self.time_allocated(resource) \
            - self.time_used(resource)
        if delta < 0:
            return -1 * delta
        else:
            return 0
    
    def credit_available (self, resource):
        return self.credit_limit(resource) \
            - self.credit_used(resource)


class Resource (object):
    
    """A logical resource.
    
    Relationships:
    credit_limits -- CreditLimits on the resource.
    requests -- Requests made for the resource.
    unit_factors -- UnitFactors for this resource.
    
    Attributes:
    id -- Canonical id of the resource.
    
    Properties:
    name -- Upstream name of the resource.
    
    Exceptions:
    DoesNotExist -- The specified resource does not exist.
    """
    
    class DoesNotExist (Exception):
        """The specified resource does not exist."""
    
    @classmethod
    def by_name (cls, name):
        """Get (or create) a resource based on its name upstream.
        
        Arguments:
        name -- The upstream name of the resource.
        """
        try:
            upstream_resource = upstream.Resource.by_name(name)
        except upstream.Resource.DoesNotExist:
            raise cls.DoesNotExist()
        try:
            return cls.query.filter_by(id=upstream_resource.id).one()
        except exceptions.InvalidRequestError:
            return cls(id=upstream_resource.id)
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.credit_limits = kwargs.get("credit_limits", [])
        self.requests = kwargs.get("requests", [])
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def __str__ (self):
        return self.name
    
    def _get_name (self):
        """Return the name of the upstream resource."""
        upstream_resource = upstream.Resource.by_id(self.id)
        return upstream_resource.name
    name = property(_get_name)
