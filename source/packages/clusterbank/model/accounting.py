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


class AccountingEntity (object):
    
    def __str__ (self):
        try:
            resource_name = self.resource.name
        except AttributeError:
            resource_name = "unknown"
        try:
            return "%s %i" % (resource_name, self.time)
        except TypeError:
            return "%r ?" % resource_name
    
    def __repr__ (self):
        try:
            return "<%s %i>" % (self.__class__.__name__, self.id)
        except TypeError:
            return "<%s ?>" % self.__class__.__name__


class CreditLimit (AccountingEntity):
    
    """A limit on the charges a project can have.
    
    Attributes:
    start -- when this credit limit becomes active
    time -- amount of credit authorized
    comment -- A verbose comment of why credit was allocated.
    project -- project that has the credit limit
    resource -- resource the credit limit is for
    poster -- user who posted the credit limit
    
    Constraints:
    unique by project, resource, and start
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.start = kwargs.get("start")
        self.time = kwargs.get("time")
        self.comment = kwargs.get("comment")
        self.project = kwargs.get("project")
        self.resource = kwargs.get("resource")
        self.poster = kwargs.get("poster")
    
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


class Request (AccountingEntity):
    
    """A request for time on a resource.
    
    Attributes:
    datetime -- when the request was entered
    time -- amount of time requested
    comment -- verbose description of need
    start -- when the allocation should become active
    open -- the request remains unanswered
    resource -- the resource to be used
    project -- the project for which time is requested
    poster -- the user requesting the time
    allocations -- allocations on the system in response to this request
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
    
    def _get_time (self):
        return self._time
    
    def _set_time (self, value):
        if value is not None:
            if value < 0:
                raise ValueError("cannot request negative time")
        self._time = value
    
    time = property(_get_time, _set_time)
    
    def _get_allocated (self):
        """Whether the request has had time allocated to it."""
        return len(self.allocations) > 0
    
    allocated = property(_get_allocated)
    
    def _get_open (self):
        """Whether the request is awaiting a reply."""
        return not self.allocated
    
    open = property(_get_open)


class Allocation (AccountingEntity):
    
    """An amount of time allocated to a project.
    
    Properties:
    request -- request for time to which this is a response
    poster -- user who entered the allocation into the system
    charges -- time used from the allocation
    datetime -- when the allocation was entered
    approver -- the person/group who approved the allocation
    time -- amount of time allocated
    start -- when the allocation becomes active
    comment -- verbose description of the allocation
    project -- project from associated request
    resource -- resource from associated request
    started -- allocation has started
    expired -- allocation has expired
    active -- allocation has started and has not expired
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
    
    def _get_poster (self):
        return self._poster
    
    def _set_poster (self, user):
        if user is not None:
            if not user.can_allocate:
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
                time_liened += lien.time or 0
        return time_liened
    
    time_liened = property(_get_time_liened)
    
    def _get_time_available (self):
        return self.time - self.time_liened - self.time_charged
    
    time_available = property(_get_time_available)


class Lien (AccountingEntity):
    
    """A potential charge against an allocation.
    
    Properties:
    datetime -- when the lien was entered
    time -- how many time could be charged
    comment -- verbose description of the lien
    project -- points to related project
    resource -- points to related resource
    effective_charge -- total time charged (after refunds)
    time_available -- difference of time and effective_charge
    charged -- the lien has charges
    active -- the lien is against an active allocation
    open -- the lien is uncharged
    allocation -- the allocation the lien is against
    poster -- the user who posted the lien
    charges -- charges resulting from the lien
    
    Methods:
    charge -- charge time against this lien
    
    Exceptions:
    InsufficientFunds -- charges exceed liens
    """
    
    class InsufficientFunds (Exception):
        """Charges exceed liens."""
    
    @classmethod
    def distributed (cls, project, resource, **kwargs):
        
        """Distribute a lien against any active allocations for a project and resource.
        
        Arguments:
        project -- project to post lien against
        resource -- resource to post lien for
        
        Keyword arguments:
        time -- time to secure in the lien
        
        Keyword arguments are passed to the constructor.
        """
        
        time = kwargs.pop("time")
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
                lien = cls(allocation=allocation, time=time, **kwargs)
            else:
                lien = cls(allocation=allocation, time=allocation.time_available, **kwargs)
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
                    raise Exception("there are no active allocations for %s on %s" % (project, resource))
                lien = Lien(allocation=allocation, time=time, **kwargs)
                liens.append(lien)
        return liens
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.allocation = kwargs.get("allocation")
        self.poster = kwargs.get("poster")
        self.datetime = kwargs.get("datetime")
        self.time = kwargs.get("time")
        self.comment = kwargs.get("comment")
        self.charges = kwargs.get("charges", [])
    
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
            if getattr(self, "allocation", None) is not None:
                prev_value = getattr(self, "_time", None)
                try:
                    self._time = 0
                    credit_limit = self.allocation.project.credit_available(self.allocation.resource)
                    if value > self.allocation.time - self.allocation.time_liened + credit_limit:
                        raise self.project.InsufficientFunds("credit limit exceeded")
                finally:
                    self._time = prev_value
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


class Charge (AccountingEntity):
    
    """A charge against an allocation.
    
    Properties:
    project -- project from related request
    resource -- resource from related request
    datetime -- when the charge was deducted
    time -- amount of time used
    comment -- a verbose description of the charge
    effective_charge -- The unit charge after any refunds
    active -- the charge is against an active lien
    lien -- the lien to which this charge applies
    poster -- who posted the transaction
    refunds -- refunds against this charge
    
    Methods:
    refund -- refund time from this charge
    """
    
    @classmethod
    def distributed (cls, liens, **kwargs):
        time = kwargs.pop("time")
        charges = list()
        for lien in liens:
            # If the remaining time will fit into the lien, post a
            # Charge for all of it. Otherwise, post a charge for what
            # the lien can support.
            if lien.time_available >= time:
                charge = Charge(lien=lien, time=time, **kwargs)
            else:
                charge = Charge(lien=lien, time=lien.time_available, **kwargs)
            charges.append(charge)
            time -= charge.time
        
        # If there is time remaining, add it to the last charge.
        if time > 0:
            try:
                charges[-1].time += time
            except IndexError:
                # No charges have yet been made. Charge the last lien.
                try:
                    charge = Charge(lien=lien, time=time, **kwargs)
                except NameError:
                    # There was no lien.
                    raise Exception("no liens are available to be charged")
                charges.append(charge)
        return charges
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.lien = kwargs.get("lien")
        self.poster = kwargs.get("poster")
        self.datetime = kwargs.get("datetime")
        self.time = kwargs.get("time")
        self.comment = kwargs.get("comment")
        self.refunds = kwargs.get("refunds", [])
    
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


class Refund (AccountingEntity):
    
    """A refund against a charge.
    
    Properties:
    project -- project from associated charge
    resource -- resource from associated charge
    charge -- charge being refunded
    datetime -- when the refund was added
    time -- amount of time refunded
    comment -- description of the refund
    poster -- who posted the refund
    active -- refund is against an active charge
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.charge = kwargs.get("charge")
        self.poster = kwargs.get("poster")
        self.datetime = kwargs.get("datetime")
        self.time = kwargs.get("time")
        self.comment = kwargs.get("comment")
    
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
    
    def _get_charge (self):
        return self._charge
    
    def _set_charge (self, charge):
        if charge is not None:
            if getattr(self, "time", None) is not None:
                prev_charge = getattr(self, "_charge", None)
                try:
                    self._charge = None
                    if self.time > charge.effective_charge:
                        raise ValueError("refunds cannot exceed charge")
                finally:
                    self._charge = prev_charge
        self._charge = charge
    
    charge = property(_get_charge, _set_charge)
    
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
                    if value > self.charge.effective_charge:
                        raise ValueError("refunds cannot exceed charge")
                finally:
                    self._time = prev_value
        self._time = value
    
    time = property(_get_time, _set_time)
    
    def _get_active (self):
        """The charge affects the project's current allocation."""
        return self.charge.active
    active = property(_get_active)
