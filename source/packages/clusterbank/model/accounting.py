"""Cluster accounting model.

Classes:
Request -- request for time on a resource
Allocation -- record of time allocated to a project
CreditLimit -- a maximum negative value for a project on a resource
Lien -- a potential charge against a allocation
Charge -- charge against a allocation
Refund -- refund against a charge
"""

from datetime import datetime

__all__ = [
    "CreditLimit", "Request", "Allocation", "Lien", "Charge", "Refund",
]


class CreditLimit (object):
    
    """A limit on the charges a project can have.
    
    Relationships:
    project -- Related project.
    resource -- Applicable resource.
    poster -- Who set this credit limit.
    
    Attributes:
    start -- When this credit limit becomes active.
    time -- Amount of credit available.
    explanation -- A verbose explanation of why credit was allocated.
    
    Constraints:
     * Only one entry for a given project, start, and resource.
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.start = kwargs.get("start")
        self.time = kwargs.get("time")
        self.comment = kwargs.get("comment")
        self.project = kwargs.get("project")
        self.resource = kwargs.get("resource")
        self.poster = kwargs.get("poster")
    
    def __str__ (self):
        return "%s ~%i" % (self.resource.name, self.time)
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _get_poster (self):
        return self._poster
    
    def _set_poster (self, user):
        if not user.can_allocate:
            raise user.NotPermitted("%s cannot allocate credit" % user)
        self._poster = user
    
    poster = property(_get_poster, _set_poster)
    
    def _get_time (self):
        return self._time
    
    def _set_time (self, value):
        if value < 0 and value is not None:
            raise ValueError("credit limit cannot be negative")
        self._time = value
    
    time = property(_get_time, _set_time)


class Request (object):
    
    """A request for time on a resource.
    
    Relationships:
    resource -- The resource to be used.
    project -- The project for which time is requested.
    poster -- The user requesting the time.
    allocations -- Allocations on the system in response to this request.
    
    Attributes:
    datetime -- When the request was entered.
    time -- Amount of time requested.
    explanation -- Verbose description of need.
    start -- When the allocation should become active.
    
    Properties:
    open -- The request remains unanswered.
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.resource = kwargs.get("resource")
        self.project = kwargs.get("project")
        self.poster = kwargs.get("poster")
        self.datetime = kwargs.get("datetime")
        self.time = kwargs.get("time")
        self.comment = kwargs.get("comment")
        self.start = kwargs.get("start")
        self.allocations = kwargs.get("allocations", [])
    
    def _get_poster (self):
        return self._poster
    
    def _set_poster (self, user):
        if not user.can_request:
            raise user.NotPermitted("%s cannot make requests" % user)
        if getattr(self, "project", None) is not None and not (user.member_of(self.project) or user.can_allocate):
            raise user.NotPermitted("%s is not a member of %s" % (user, self.project))
        self._poster = user
    
    poster = property(_get_poster, _set_poster)
    
    def _get_project (self):
        return self._project
    
    def _set_project (self, project):
        if getattr(self, "user", None) is not None and not (self.user.member_of(project) or self.user.can_allocate):
            raise self.user.NotPermitted("%s is not a member of %s" % (self.user, project))
        self._project = project
    
    project = property(_get_project, _set_project)
    
    def __str__ (self):
        try:
            resource_name = self.resource.name
        except Resource.DoesNotExist:
            resource_name = None
        return "%s ?%s" % (resource_name, self.time)
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _check_values (self):
        """Check that the values of the request are valid."""
        if self.time is not None and self.time < 0:
            raise ValueError("Cannot request negative time.")
    
    def _get_allocated (self):
        """Whether the request has had time allocated to it."""
        return len(self.allocations) > 0
    allocated = property(_get_allocated)
    
    def _get_open (self):
        """Whether the request is awaiting a reply."""
        return not self.allocated
    open = property(_get_open)


class Allocation (object):
    
    """An amount of time allocated to a project.
    
    Relationships:
    request -- The request for time to which this is a response.
    poster -- User who entered the allocation into the system.
    charges -- Time used from the allocation.
    
    Attributes:
    datetime -- When the allocation was entered.
    approver -- The person/group who approved the allocation.
    time -- Amount of time allocated.
    start -- When the allocation becomes active.
    explanation -- Verbose description of the allocation.
    
    Properties:
    project -- Project from associated request.
    resource -- Resource from associated request.
    started -- Allocation has started.
    expired -- Allocation has expired.
    active -- The allocation has started and has not expired.
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.request = kwargs.get("request")
        self.poster = kwargs.get("poster")
        self.approver = kwargs.get("approver")
        self.datetime = kwargs.get("datetime")
        self.time = kwargs.get("time")
        if self.time is None and self.request:
            self.time = self.request.time
        self.start = kwargs.get("start")
        self.expiration = kwargs.get("expiration")
        self.comment = kwargs.get("comment")
        self.liens = kwargs.get("liens", [])
    
    def __str__ (self):
        return "%s +%i" % (self.resource.name, self.time)
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _get_poster (self):
        return self._poster
    
    def _set_poster (self, user):
        if user is not None and not user.can_allocate:
            raise user.NotPermitted("%s cannot allocate time" % user)
        self._poster = user
    
    poster = property(_get_poster, _set_poster)
    
    def _get_project (self):
        """Return the related project."""
        return self.request.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.request.resource
    resource = property(_get_resource)
    
    def _get_time (self):
        return self._time
    
    def _set_time (self, value):
        if value is not None:
            if value < 0:
                raise ValueError("cannot allocate negative time")
        self._time = value
    
    time = property(_get_time, _set_time)
        
    def _set_programmatic_defaults (self):
        if not self.start:
            self.start = self.request.start
    
    def _get_started (self):
        """The allocation has a start date before now."""
        return self.start <= datetime.now()
    started = property(_get_started)
    
    def _get_expired (self):
        """The allocation has an expiration date before now."""
        return self.expiration <= datetime.now()
    expired = property(_get_expired)
    
    def _get_active (self):
        """The allocation's time affect's the project's time."""
        return self.started and not self.expired
    active = property(_get_active)
    
    def _get_charges (self):
        """Return the set of charges made against this allocation."""
        return Charge.query.join("lien").filter_by(allocation=self)
    charges = property(_get_charges)
    
    def _get_time_charged (self):
        """Return the sum of effective charges against this allocation."""
        time_charged = 0
        for charge in self.charges:
            time_charged += charge.effective_charge
        return time_charged
    time_charged = property(_get_time_charged)
    
    def _get_time_liened (self):
        """Sum of time in open liens."""
        time_liened = 0
        for lien in self.liens:
            if lien.open:
                time_liened += lien.time
        return time_liened
    time_liened = property(_get_time_liened)
    
    def _get_time_available (self):
        return self.time \
            - self.time_liened \
            - self.time_charged
    time_available = property(_get_time_available)


class Lien (object):
    
    """A potential charge against an allocation.
    
    Relationships:
    allocation -- The allocation the lien is against.
    poster -- The user who posted the lien.
    charges -- Charges resulting from the lien.
    
    Attributes:
    datetime -- When the lien was entered.
    time -- How many time could be charged.
    explanation -- Verbose description of the lien.
    
    Properties:
    project -- Points to related project.
    resource -- Points to related resource.
    effective_charge -- Total time charged (after refunds).
    time_available -- Difference of time and effective_charge.
    charged -- The lien has charges.
    active -- The lien is against an active allocation.
    open -- The lien is uncharged.
    
    Methods:
    charge -- Charge time against this lien.
    
    Exceptions:
    InsufficientFunds -- Charges exceed liens.
    """
    
    class InsufficientFunds (Exception):
        """Charges exceed liens."""
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.allocation = kwargs.get("allocation")
        self.poster = kwargs.get("poster")
        self.datetime = kwargs.get("datetime")
        self.time = kwargs.get("time")
        self.comment = kwargs.get("comment")
        self.charges = kwargs.get("charges", [])
    
    def __str__ (self):
        return "%s %i/%i" % (
            self.resource.name,
            self.effective_charge, self.time
        )
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _get_poster (self):
        return self._poster
    
    def _set_poster (self, user):
        if not user.can_lien:
            raise user.NotPermitted("%s cannot post liens" % user)
        if getattr(self, "project", None) is not None and not (user.member_of(self.project) or user.can_charge):
            raise user.NotPermitted("%s is not a member of %s" % (user, self.project))
        self._poster = user
    
    poster = property(_get_poster, _set_poster)
    
    def _get_time (self):
        return self._time
    
    def _set_time (self, value):
        """Check that the value of the lien is valid."""
        if value is not None:
            if value < 0:
                raise ValueError("lien cannot be for negative time")
            if getattr(self, "project", None) is not None and getattr(self, "resource", None) is not None:
                pre_value = getattr(self, "_time", None)
                try:
                    self._time = 0
                    if value > self.project.time_available(self.resource) + self.project.credit_available(self.resource):
                        raise self.project.InsufficientFunds("credit limit exceeded")
                finally:
                    self._time = pre_value
        self._time = value
    
    time = property(_get_time, _set_time)
    
    def _get_project (self):
        """Return the related project."""
        return self.allocation.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.allocation.resource
    resource = property(_get_resource)
    
    def _get_effective_charge (self):
        """Sum the effective charges of related charges for this lien."""
        effective_charge = 0
        for charge in self.charges:
            effective_charge += charge.effective_charge
        return effective_charge
    effective_charge = property(_get_effective_charge)
    
    def _get_time_available (self):
        """Difference of time liened and effective charge."""
        return self.time - self.effective_charge
    time_available = property(_get_time_available)
    
    def _get_charged (self):
        """The lien has been charged."""
        return len(self.charges) > 0
    charged = property(_get_charged)
    
    def _get_active (self):
        """The lien affects the current allocation."""
        return self.allocation.active
    active = property(_get_active)
    
    def _get_open (self):
        """The lien is still awaiting charges."""
        return not self.charged
    open = property(_get_open)


class Charge (object):
    
    """A charge against an allocation.
    
    Relationships:
    lien -- The lien to which this charge applies.
    poster -- Who posted the transaction.
    refunds -- Refunds against this charge.
    
    Attributes:
    datetime -- When the charge was deducted.
    time -- Amount of time used.
    explanation -- A verbose description of the charge.
    
    Properties:
    effective_charge -- The unit charge after any refunds.
    project -- project from related request.
    resource -- resource from related request.
    active -- The charge is against an active lien.
    
    Methods:
    refund -- Refund time from this charge.
    
    Exceptions:
    ExcessiveRefund -- Refund in excess of charge.
    """
    
    class ExcessiveRefund (Exception):
        """Refund in excess of charge."""
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.lien = kwargs.get("lien")
        self.poster = kwargs.get("poster")
        self.datetime = kwargs.get("datetime")
        self.time = kwargs.get("time")
        self.comment = kwargs.get("comment")
        self.refunds = kwargs.get("refunds", [])
    
    def __str__ (self):
        return "%s -%s" % (
            self.resource.name,
            self.effective_charge
        )
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _get_poster (self):
        return self._poster
    
    def _set_poster (self, user):
        if user is not None and not user.can_charge:
            raise user.NotPermitted("%s cannot post charges" % user)
        self._poster = user
    
    poster = property(_get_poster, _set_poster)
    
    def _get_effective_charge (self):
        """Difference of charge time and refund times."""
        effective_charge = self.time or 0
        for refund in Refund.query.filter_by(charge=self):
            effective_charge -= refund.time
        return effective_charge
    effective_charge = property(_get_effective_charge)
    
    def _get_project (self):
        """Return the related project."""
        return self.lien.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.lien.resource
    resource = property(_get_resource)
    
    def _get_time (self):
        return self._time
    
    def _set_time (self, value):
        """Check that the values of the charge are valid."""
        if value is not None:
            if value < 0:
                raise ValueError("cannot charge negative time")
        self._time = value
    
    time = property(_get_time, _set_time)
    
    def _get_active (self):
        """Charge affects the project's current allocation."""
        return self.lien.active
    active = property(_get_active)


class Refund (object):
    
    """A refund against a charge.
    
    Relationships:
    charge -- The charge being refunded.
    poster -- Who posted the refund.
    
    Attributes:
    datetime -- When the refund was added.
    time -- How much time was refunded.
    explanation -- A (possibly verbose) description of the refund.
    
    Properties:
    project -- Project from associated charge.
    resource -- Resource from associated charge.
    active -- The refund is against an active charge.
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.charge = kwargs.get("charge")
        self.poster = kwargs.get("poster")
        self.datetime = kwargs.get("datetime")
        self.time = kwargs.get("time")
        self.comment = kwargs.get("comment")
    
    def __str__ (self):
        return "%s +%i" % (self.resource.name, self.time)
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _get_project (self):
        """Return the related project."""
        return self.charge.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.charge.resource
    resource = property(_get_resource)
    
    def _get_poster (self):
        return self._poster
    
    def _set_poster (self, user):
        if not user.can_refund:
            raise user.NotPermitted("%s cannot refund charges" % user)
        self._poster = user
    
    poster = property(_get_poster, _set_poster)
    
    def _get_time (self):
        return self._time
    
    def _set_time (self, value):
        if value is not None:
            if value < 0:
                raise ValueError("cannot refund negative time")
            elif getattr(self, "charge", None) is not None:
                prev_value = getattr(self, "_time", None)
                try:
                    self._time = 0
                    print self
                    if value > self.charge.effective_charge:
                        raise self.charge.ExcessiveRefund("refunds cannot exceed charges")
                finally:
                    self._time = prev_value
        self._time = value
    
    time = property(_get_time, _set_time)
    
    def _get_active (self):
        """The charge affects the project's current allocation."""
        return self.charge.active
    active = property(_get_active)
