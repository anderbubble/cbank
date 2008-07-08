"""model entities

Classes:
Resource -- resource that can be allocated
Project -- project to which resources can be allocated
Request -- request for amount on a resource
Allocation -- record of amount allocated to a project
CreditLimit -- a maximum negative value for a project on a resource
Hold -- a potential charge against a allocation
Charge -- charge against a allocation
Refund -- refund of a charge
"""

from datetime import datetime
import ConfigParser
import operator
try:
    set
except NameError:
    from sets import Set as set
import warnings

from clusterbank import config
import clusterbank.exceptions as exceptions
import clusterbank.model

__all__ = [
    "User", "Project", "Resource",
    "Request", "Allocation", "CreditLimit", "Hold", "Charge", "Refund"
]


config.read(["/etc/clusterbank.conf"])


class UpstreamProxy (object):

    def __init__ (self, use=None):
        self.use = use

    def __getattr__ (self, name):
        return getattr(self.use, name)

upstream = UpstreamProxy()


class Entity (object):
    
    def __repr__ (self):
        return "<%s id=%r>" % (self.__class__.__name__, self.id)


class UpstreamEntity (Entity):
    
    def __str__ (self):
        if self.name is not None:
            return str(self.name)
        else:
            return "?"
    
    def __repr__ (self):
        return "<%s name=%r id=%r>" % (self.__class__.__name__, self.name, self.id)
    

class User (UpstreamEntity):
    
    """User associated with a hold or charge.
    
    Properties:
    id -- unique integer identifier
    name -- canonical name of the user (from upstream)
    is_admin -- whether the user has admin privileges
    """
    
    def __init__ (self, **kwargs):
        """Initialize a user.
        
        Keyword arguments:
        id -- unique integer identifier
        """
        self.id = kwargs.get("id")
        self.holds = kwargs.get("holds", [])
        self.charges = kwargs.get("charges", [])
    
    def _get_name (self):
        return upstream.get_user_name(self.id)
    
    name = property(_get_name)
    
    def _get_project_ids (self):
        return upstream.get_member_projects(self.id)
    
    def _get_owned_project_ids (self):
        return upstream.get_owner_projects(self.id)
    
    def _get_is_admin (self):
        try:
            admins = config.get("cbank", "admins")
        except ConfigParser.Error:
            admins = []
        else:
            admins = admins.split(",")
        return self.name in admins
    
    is_admin = property(_get_is_admin)


class Project (UpstreamEntity):
    
    """Project to which resources can be allocated.
    
    Properties:
    id -- unique integer identifier
    name -- canonical name of the project (from upstream)
    requests -- request from the project
    allocations -- allocations to the project
    credit_limits -- credit limits for the project
    credit_limit -- credit limit for a resource at a given datetime
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
    
    def _get_name (self):
        return upstream.get_project_name(self.id)
    
    name = property(_get_name)

    def _get_member_ids (self):
        return upstream.get_project_members(self.id)
    
    def _get_owner_ids (self):
        return upstream.get_project_owners(self.id)
    
    def credit_limit (self, resource, datetime=datetime.now):
        try:
            datetime = datetime()
        except TypeError:
            pass
        credit_limits = [limit for limit in self.credit_limits
            if limit.resource==resource and limit.start<=datetime]
        credit_limits = sorted(credit_limits, key=operator.attrgetter("start"))
        try:
            return credit_limits[0]
        except IndexError:
            return None


class Resource (UpstreamEntity):
    
    """Resource that can be allocated to a project.
    
    Properties:
    id -- canonical id of the resource
    name -- canonical name of the resource (from upstream)
    requests -- requests for the resource
    allocations -- allocations of the resource
    credit_limits -- credit limits on the resource
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
    
    def _get_name (self):
        return upstream.get_resource_name(self.id)
    
    name = property(_get_name)

class Request (Entity):
    
    """A request for amount on a resource.
    
    Properties:
    id -- unique integer identifier
    project -- project making the request
    resource -- resource for which an allocation is request
    datetime -- timestamp for when this request was entered
    amount -- amount requested
    start -- date when the allocation is needed
    comment -- misc. comments
    allocations -- allocations that was made in response to the request
    """
    
    def __init__ (self, **kwargs):
        """Initialize a new request.
        
        Keyword arguments:
        id -- unique integer identifier
        project -- project making the request
        resource -- resource for which an allocation is requested
        datetime -- timestamp for when this request was entered
        amount -- amount requested
        start -- date when the allocation is needed
        comment -- misc. comments
        allocations -- allocations that was made in response to the request
        """
        self.datetime = kwargs.get("datetime")
        self.id = kwargs.get("id")
        self.project = kwargs.get("project")
        self.resource = kwargs.get("resource")
        self.amount = kwargs.get("amount")
        self.start = kwargs.get("start")
        self.comment = kwargs.get("comment")
        self.allocations = kwargs.get("allocations", [])


class Allocation (Entity):
    
    """An amount of a resource allocated to a project.
    
    Properties:
    id -- unique integer identifier
    project -- project to which the resource has been allocated
    resource -- resource allocated
    datetime -- when the allocation was entered
    amount -- amount allocated
    start -- when the allocation becomes active
    expiration -- when the allocation expires
    comment -- misc. comments
    requests -- requests answered by this allocation
    holds -- holds on this allocation
    charges -- charges against this allocation
    """
    
    def __init__ (self, **kwargs):
        """Initialize a new allocation.
        
        Keyword arguments:
        request -- request prompting this allocation
        id -- unique integer identifier
        project -- project to which the resource has been allocated
        resource -- resource allocated
        datetime -- when the allocation was entered
        amount -- amount allocated
        start -- when the allocation becomes active
        expiration -- when the allocation expires
        comment -- misc. comments
        requests -- requests answered by this allocation
        holds -- holds on this allocation
        charges -- charges against this allocation
        amount_available -- allocated amount after charges, refunds, and holds
        active -- current time is between start and expiration
        
        If a request is specified, and the request has no allocation, the new
        allocation will be related to the request.
        """
        self.datetime = kwargs.get("datetime")
        if kwargs.get("request") is not None:
            request = kwargs.get("request")
            if request.allocation is None:
                request.allocation = self
        self.id = kwargs.get("id")
        self.project = kwargs.get("project")
        self.resource = kwargs.get("resource")
        self.amount = kwargs.get("amount")
        self.start = kwargs.get("start")
        self.expiration = kwargs.get("expiration")
        self.comment = kwargs.get("comment")
        self.requests = kwargs.get("requests", [])
        self.holds = kwargs.get("holds", [])
        self.charges = kwargs.get("charges", [])
    
    def _get_amount_charged (self):
        return sum(charge.effective_amount for charge in self.charges)
    
    amount_charged = property(_get_amount_charged)
    
    def _get_amount_held (self):
        return sum(hold.amount or 0 for hold in self.holds if hold.active)
    
    amount_held = property(_get_amount_held)
    
    def _get_amount_available (self):
        return self.amount - (self.amount_charged + self.amount_held)
    
    amount_available = property(_get_amount_available)
    
    def _get_active (self):
        """Intelligent property accessor."""
        return self.start <= datetime.now() < self.expiration
    
    active = property(_get_active)


class CreditLimit (Entity):
    
    """A credit limit for charges by a project on a resource.
    
    Properties:
    id -- unique integer identifier
    project -- project to which the credit limit applies
    resource -- resource to which the credit limit applies
    datetime -- when the credit limit was entered
    start -- when the credit limit goes into effect
    amount -- amount available through credit
    comment -- misc. comments
    
    Constraints:
    unique by project, resource, and start
    """
    
    def __init__ (self, **kwargs):
        """Initialize a new credit limit.
        
        Keyword arguments:
        id -- unique integer identifier
        project -- project to which the credit limit applies
        resource -- resource to which the credit limit applies
        datetime -- when the credit limit was entered
        start -- when the credit limit goes into effect
        amount -- amount available through credit
        comment -- misc. comments
        """
        self.datetime = kwargs.get("datetime")
        self.id = kwargs.get("id")
        self.project = kwargs.get("project")
        self.resource = kwargs.get("resource")
        self.start = kwargs.get("start")
        self.amount = kwargs.get("amount")
        self.comment = kwargs.get("comment")


class Hold (Entity):
    
    """Uncharged but unavailable amount of an allocation.
    
    Example:
    A hold may be placed on an account when a job starts, and may be replaced
    with a charge when the job finishes.
    
    Properties:
    id -- unique integer identifier
    allocation -- allocation to which the hold applies
    datetime -- when the hold was entered
    amount -- amount held
    comment -- misc. comments
    active -- the hold is active
    
    Classmethods:
    distributed -- construct multiple holds across multiple allocations
    """
    
    def __init__ (self, **kwargs):
        """Initialize a new hold.
        
        Keyword arguments:
        id -- unique integer identifier
        allocation -- allocation to which the hold applies
        datetime -- when the hold was entered
        amount -- amount held
        comment -- misc. comments
        active -- the hold is active
        """
        self.datetime = kwargs.get("datetime")
        self.id = kwargs.get("id")
        self.allocation = kwargs.get("allocation")
        self.comment = kwargs.get("comment")
        self.active = kwargs.get("active", True)
        self.amount = kwargs.get("amount")
        self.user = kwargs.get("user")
    
    @classmethod
    def distributed (cls, allocations, **kwargs):
        
        """Construct multiple holds across multiple allocations.
        
        Arguments:
        allocations -- a list of allocations available for holds
        
        Keyword arguments:
        amount -- total amount to be held (required)
        *additional keyword arguments are used to construct each hold
        
        Example:
        A project has multiple allocations on a single resource. Use a
        distributed hold to easily hold more funds than any one allocation can
        accomodate.
        """
        
        amount = kwargs.pop("amount")
        
        holds = list()
        for allocation in allocations:
            if allocation.amount_available <= 0:
                continue
            if allocation.amount_available >= amount:
                hold = cls(allocation=allocation, amount=amount, **kwargs)
            else:
                hold = cls(allocation=allocation, amount=allocation.amount_available, **kwargs)
            holds.append(hold)
            amount -= hold.amount
            if amount <= 0:
                break
        
        if amount > 0:
            try:
                hold = holds[-1]
            except IndexError:
                try:
                    allocation = allocations[0]
                except IndexError:
                    raise ValueError("no allocation to hold on")
                else:
                    hold = cls(allocation=allocation, amount=amount, **kwargs)
                    holds.append(hold)
                    amount = 0
            else:
                hold.amount += amount
        return holds


class Charge (Entity):
    
    """A charge against an allocation.
    
    Properties:
    id -- unique integer identifier
    allocation -- allocation to which the charge applies
    amount -- amount charged
    datetime -- when the chage was entered
    comment -- misc. comments
    refunds -- refunds from the charge
    
    Classmethods:
    distributed -- construct multiple charges across multiple allocations
    """
    
    def __init__ (self, **kwargs):
        """Initialize a new charge.
        
        Keyword arguments:
        id -- unique integer identifier
        allocation -- allocation to which the charge applies
        amount -- amount charged
        effective_amount -- amount after refunds
        datetime -- when the chage was entered
        comment -- misc. comments
        refunds -- refunds from the charge
        """
        self.datetime = kwargs.get("datetime")
        self.id = kwargs.get("id")
        self.allocation = kwargs.get("allocation")
        self.user = kwargs.get("user")
        self.comment = kwargs.get("comment")
        self.refunds = kwargs.get("refunds", [])
        self.amount = kwargs.get("amount")
        if kwargs.get("hold") is not None:
            hold = kwargs.get("hold")
            hold.active = False
            if self.allocation is None:
                self.allocation = hold.allocation
            if self.amount is None:
                self.amount = hold.amount
    
    @classmethod
    def distributed (cls, allocations, **kwargs):
        
        """Construct multiple charges across multiple allocations.
        
        Arguments:
        allocations -- a list of allocations available for charges
        
        Keyword arguments:
        amount -- total amount to be charged (required)
        *additional keyword arguments are used to construct each charge
        
        Example:
        A project has multiple allocations on a single resource. Use a
        distributed charge to easily charge more funds than any one allocation can
        accomodate.
        """
        
        amount = kwargs.pop("amount")
        
        charges = list()
        for allocation in allocations:
            if allocation.amount_available <= 0:
                continue
            if allocation.amount_available >= amount:
                charge = cls(allocation=allocation, amount=amount, **kwargs)
            else:
                charge = cls(allocation=allocation, amount=allocation.amount_available, **kwargs)
            amount -= charge.amount
            charges.append(charge)
            if amount <= 0:
                break
        
        if amount > 0:
            try:
                charge = charges[-1]
            except IndexError:
                try:
                    allocation = allocations[0]
                except IndexError:
                    raise ValueError("no allocations to charge against")
                else:
                    charge = cls(allocation=allocation, amount=amount, **kwargs)
                    charges.append(charge)
                    amount = 0
            else:
                charge.amount += amount
        
        return charges
    
    def _get_effective_amount (self):
        amount_refunded = sum(refund.amount or 0 for refund in self.refunds)
        return self.amount - amount_refunded
    
    effective_amount = property(_get_effective_amount)


class Refund (Entity):
    
    """A refund against a charge.
    
    Properties:
    id -- unique integer identifier
    charge -- charge being refunded
    datetime -- when the charge was entered
    amount -- amount refunded
    comment -- misc. comments
    """
    
    def __init__ (self, **kwargs):
        """Initialize a new refund.
        
        Keyword arguments:
        id -- unique integer identifier
        charge -- charge being refunded
        datetime -- when the charge was entered
        amount -- amount refunded (default: charge amount)
        comment -- misc. comments
        """
        self.datetime = kwargs.get("datetime")
        self.id = kwargs.get("id")
        self.charge = kwargs.get("charge")
        self.comment = kwargs.get("comment")
        self.amount = kwargs.get("amount")
        if self.amount is None and self.charge is not None:
            self.amount = self.charge.effective_amount
