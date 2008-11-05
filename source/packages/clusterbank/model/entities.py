"""model entities

Classes:
User -- user that can use resources
Project -- project to which resources can be allocated
Resource -- resource that can be allocated
Allocation -- record of amount allocated to a project
Hold -- a potential charge against a allocation
Charge -- charge against a allocation
Refund -- refund of a charge
"""

from datetime import datetime
import ConfigParser

from clusterbank import config


__all__ = [
    "upstream", "User", "Project", "Resource",
    "Allocation", "Hold", "Charge", "Refund"
]


class UpstreamProxy (object):
    
    """A proxy for upstream modules.
    
    An upstream proxy provides a mechanism for  generic coupling with
    upstream modules.
    
    Attributes:
    use -- the upstream module to proxy to
    """

    def __init__ (self, use=None):
        """Initialize an UpstreamProxy.
        
        Arguments:
        use -- the upstream module to proxy to
        """
        self.use = use

    def __getattr__ (self, name):
        return getattr(self.use, name)


upstream = UpstreamProxy()


class Entity (object):
    
    """A generic accounting entity.
    
    Subclasses of this class represent the elements of accounting.
    
    Attributes:
    id -- the entity id
    """
    
    def __init__ (self):
        self.id = None
    
    def __repr__ (self):
        return "<%s id=%r>" % (self.__class__.__name__, self.id)
    
    def __str__ (self):
        if self.id is not None:
            id_ = self.id
        else:
            id_ = "?"
        return "#%s" % id_


class UpstreamEntity (Entity):
    
    """A generic upstream entity.
    
    Subclasses of this class represent the organizational entities
    defined elsewhere.
    
    Attributes:
    id -- the entity id (used to locate the entity in the upstream module)
    
    Properties:
    name -- override in a subclass
    """
    
    name = property(lambda:None)
    
    def __init__ (self, id_):
        """Initialize an UpstreamEntity.
        
        Arguments:
        id_ -- the id of the upstream entity
        """
        Entity.__init__(self)
        self.id = id_
    
    def __repr__ (self):
        return "<%s name=%r id=%r>" % (
            self.__class__.__name__, self.name, self.id)
    
    def __str__ (self):
        if self.name is not None:
            return str(self.name)
        else:
            return "?"


class User (UpstreamEntity):
    
    """User associated with a hold or charge.
    
    Properties:
    name -- upstream name of the user
    projects -- upstream project ids the user is a member of
    projects_owned -- upstream project ids the user owns
    is_admin -- whether the user has admin privileges
    """
    
    def __init__ (self, id_):
        """Initialize an upstream-backed user.
        
        Arguments:
        id_ -- the user's upstream id
        """
        UpstreamEntity.__init__(self, id_)
        self.holds = []
        self.charges = []
    
    def _get_name (self):
        """Retrieve the user's name from upstream."""
        return upstream.get_user_name(self.id)
    
    name = property(_get_name)
    
    def _get_projects (self):
        """Retrieve the ids of the user's projects from upstream."""
        return upstream.get_member_projects(self.id)
    
    projects = property(_get_projects)
    
    def _get_projects_owned (self):
        """Retrieve the ids of the projects owned by the user from upstream."""
        return upstream.get_owner_projects(self.id)
    
    projects_owned = property(_get_projects_owned)
    
    def _get_is_admin (self):
        """Whether or not the user is configured as an admin."""
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
    
    Attributes:
    allocations -- allocations to the project
    
    Properties:
    name -- upstream name of the project
    members -- upstream user ids of the project's members
    owners -- upstream user ids of the project's owners
    
    Methods:
    charge -- charge the project's active allocations
    """
    
    def __init__ (self, id_):
        """Initialize an upstream-backed project.
        
        Arguments:
        id_ -- the id of the project
        """
        UpstreamEntity.__init__(self, id_)
        self.allocations = []
    
    def _get_name (self):
        """Retrieve the project's name from upstream."""
        return upstream.get_project_name(self.id)
    
    name = property(_get_name)

    def _get_members (self):
        """Retrieve the ids of the project's users from upstream."""
        return upstream.get_project_members(self.id)
    
    members = property(_get_members)
    
    def _get_owners (self):
        """Retrieve the ids of the project's owners from upstream."""
        return upstream.get_project_owners(self.id)
    
    owners = property(_get_owners)
    
    def charge (self, resource, amount):
        """Charge any available allocation to the project for a given amount
        of a resource.
        
        Arguments:
        resource -- the resource the charge is for
        amount -- how much to charge
        """
        now = datetime.now()
        allocations = [allocation for allocation in self.allocations
            if allocation.start <= now and allocation.expiration > now
            and allocation.resource == resource]
        charges = Charge.distributed(allocations, amount)
        return charges


class Resource (UpstreamEntity):
    
    """Resource that can be allocated to a project.
    
    Attributes:
    allocations -- allocations of the resource
    
    Properties:
    name -- upstream name of the resource
    """
    
    def __init__ (self, id_):
        """Initialize an upstream-backed resource.
        
        Arguments:
        id_ -- the id of the resource
        """
        UpstreamEntity.__init__(self, id_)
        self.allocations = []

    def _get_name (self):
        """Retrieve the resource's name from upstream."""
        return upstream.get_resource_name(self.id)
    
    name = property(_get_name)


class Allocation (Entity):
    
    """An amount of a resource allocated to a project.
    
    Attributes:
    project -- project to which the resource has been allocated
    resource -- resource allocated
    datetime -- when the allocation was entered
    amount -- amount allocated
    start -- when the allocation becomes active
    expiration -- when the allocation expires
    comment -- misc. comments
    holds -- holds on this allocation
    charges -- charges against this allocation
    
    Properties:
    active -- if the allocation is currently active
    
    Methods:
    amount_charged -- the sum of effective charges
    amount_held -- the sum of active holds
    amount_available -- the amount available to be charged
    """
    
    def __init__ (self, project, resource, amount, start, expiration):
        """Initialize a new resource allocation.
        
        Arguments:
        project -- the project to allocate to
        resource -- the resource allocated
        amount -- how much of the resource to allocate
        start -- when the allocation becomes active
        expiration -- when the allocation is no longer active
        """
        Entity.__init__(self)
        self.datetime = datetime.now()
        self.project = project
        self.resource = resource
        self.amount = amount
        self.start = start
        self.expiration = expiration
        self.comment = None
        self.holds = []
        self.charges = []
    
    def amount_charged (self):
        """Compute the sum of effective charges (after refunds)."""
        return sum(charge.effective_amount() for charge in self.charges)
    
    def amount_held (self):
        """Compute the sum of the effective amount currently on hold."""
        return sum(hold.amount or 0 for hold in self.holds if hold.active)
    
    def amount_available (self):
        """Compute the amount available for charges."""
        return self.amount - (self.amount_charged() + self.amount_held())
    
    def _get_active (self):
        """Determine whether or not this allocation is still active."""
        return self.start <= datetime.now() < self.expiration
    
    active = property(_get_active)


class Hold (Entity):
    
    """Uncharged but unavailable amount of an allocation.
    
    Example:
    A hold may be placed on an account when a job starts, and may be replaced
    with a charge when the job finishes.
    
    Attributes:
    allocation -- allocation to which the hold applies
    datetime -- when the hold was entered
    amount -- amount held
    comment -- misc. comments
    active -- the hold is active
    
    Classmethods:
    distributed -- construct multiple holds across multiple allocations
    """
    
    def __init__ (self, allocation, amount):
        """Initialize a new hold.
        
        Arguments:
        allocation -- the allocation to hold an amount of
        amount -- how much to hold
        """
        Entity.__init__(self)
        self.datetime = datetime.now()
        self.allocation = allocation
        self.comment = None
        self.active = True
        self.amount = amount
        self.user = None
    
    @classmethod
    def distributed (cls, allocations, amount):
        
        """Construct multiple holds across multiple allocations.
        
        Arguments:
        allocations -- a list of allocations available for holds
        
        Keyword arguments:
        amount -- total amount to be held (required)
        
        Example:
        A project has multiple allocations on a single resource. Use a
        distributed hold to easily hold more funds than any one allocation can
        accomodate.
        """
        
        holds = list()
        for allocation in allocations:
            amount_available = allocation.amount_available()
            if amount_available <= 0:
                continue
            if amount_available >= amount:
                hold = cls(allocation, amount)
            else:
                hold = cls(allocation, amount_available)
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
                    hold = cls(allocation=allocation, amount=amount)
                    holds.append(hold)
                    amount = 0
            else:
                hold.amount += amount
        return holds


class Job (Entity):
    
    """A job run on a computational resource.
    
    Attributes:
    id -- the canonical job id
    user -- the user under which the job executed
    group -- the group under which the job executed
    account -- if the job has an account name string
    name -- the name of the job
    queue -- the name of the queue in which the job executed
    reservation_name -- the name of the resource reservation
    reservation_id -- the id of the resource reservation
    ctime -- when job was created (first submitted)
    qtime -- when job was queued into current queue
    etime -- when job became eligible to run
    start -- when job execution started
    exec_host -- name of host on which the job is being executed
    resource_list -- the specified resource limits
    session -- session number of job
    alternate_id -- optional alternate job identifier
    end -- when job execution ended
    exit_status -- the exit status of the job
    resources_used -- aggregate amount of specified resources used
    accounting_id -- CSA JID, job ID
    charges -- charges associated with the job
    """
    
    def __init__ (self, id_):
        """Initialize a new job.
        
        Arguments:
        resource -- the resource the job ran on
        id -- the job id
        """
        Entity.__init__(self)
        self.id = id_
        self.user = None
        self.group = None
        self.account = None
        self.name = None
        self.queue = None
        self.reservation_name = None
        self.reservation_id = None
        self.ctime = None
        self.qtime = None
        self.etime = None
        self.start = None
        self.exec_host = None
        self.resource_list = {}
        self.session = None
        self.alternate_id = None
        self.end = None
        self.exit_status = None
        self.resources_used = {}
        self.accounting_id = None
        self.charges = []
    
    def __str__ (self):
        return str(self.id)


class Charge (Entity):
    
    """A charge against an allocation.
    
    Attributes:
    allocation -- allocation to which the charge applies
    amount -- amount charged
    datetime -- when the chage was entered
    comment -- misc. comments
    refunds -- refunds from the charge
    
    Classmethods:
    distributed -- construct multiple charges across multiple allocations
    
    Methods:
    transfer -- transfer a charged amount to allocations of another project
    amount_refunded -- the sum of refunds of this charge
    effective_amount -- the effective amount of the charge (after refunds)
    """
    
    def __init__ (self, allocation, amount):
        """Initialize a new charge.
        
        Arguments:
        allocation -- the allocation to charge
        amount -- the amount to charge
        """
        Entity.__init__(self)
        self.datetime = datetime.now()
        self.allocation = allocation
        self.user = None
        self.comment = None
        self.refunds = []
        self.amount = amount
    
    @classmethod
    def distributed (cls, allocations, amount):
        
        """Construct multiple charges across multiple allocations.
        
        Arguments:
        allocations -- a list of allocations available for charges
        amount -- total amount to be charged
        
        Example:
        A project has multiple allocations on a single resource. Use a
        distributed charge to charge more funds than any one allocation can
        accomodate.
        """
        
        charges = []
        for allocation in allocations:
            amount_available = allocation.amount_available()
            if amount_available <= 0:
                continue
            if amount_available >= amount:
                charge = cls(allocation, amount)
            else:
                charge = cls(allocation, amount_available)
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
                    charge = cls(allocation, amount)
                    charges.append(charge)
                    amount = 0
            else:
                charge.amount += amount
        
        return charges
    
    def amount_refunded (self):
        """Compute the sum of refunds of the charge."""
        return sum(refund.amount or 0 for refund in self.refunds)
    
    def effective_amount (self):
        """Compute the difference between the charge and refunds."""
        return self.amount - self.amount_refunded()
    
    def transfer (self, project, amount=None):
        """Transfer a charge to allocations of another project.
        
        Arguments:
        project -- the project to transfer the charge to
        amount -- the amount to tranfer (default all)
        """
        if amount is None:
            amount = self.effective_amount()
        charges = project.charge(self.allocation.resource, amount)
        for charge in charges:
            charge.user = self.user
            charge.comment = self.comment
        refund = self.refund(amount)
        refund.comment = "transferred to %s" % project
        return refund, charges
    
    def refund (self, amount=None):
        """Refund an amount of a charge.
        
        Arguments:
        amount (default all of effective charge)
        """
        return Refund(self, amount)


class Refund (Entity):
    
    """A refund of a charge.
    
    Attributes:
    charge -- charge being refunded
    datetime -- when the charge was entered
    amount -- amount refunded
    comment -- misc. comments
    """
    
    def __init__ (self, charge, amount=None):
        """Initialize a new refund.
        
        Attributes:
        charge -- the charge refunded
        amount -- the amount refunded
        """
        Entity.__init__(self)
        self.datetime = datetime.now()
        self.charge = charge
        if amount is not None:
            self.amount = amount
        else:
            self.amount = charge.effective_amount()
        self.comment = None

