"""Cluster accounting model.

Classes:
Request -- request for amount on a resource
Allocation -- record of amount allocated to a project
CreditLimit -- a maximum negative value for a project on a resource
Hold -- a potential charge against a allocation
Charge -- charge against a allocation
Refund -- refund against a charge
"""

from datetime import datetime

__all__ = [
    "CreditLimit", "Request", "Allocation", "Hold", "Charge", "Refund",
]


class AccountingEntity (object):
    
    def __str__ (self):
        try:
            resource_name = self.resource.name
        except AttributeError:
            resource_name = "?"
        try:
            return "%s %i" % (resource_name, self.amount)
        except TypeError:
            return "%s ?" % resource_name
    
    def __repr__ (self):
        try:
            return "<%s %i>" % (self.__class__.__name__, self.id)
        except TypeError:
            return "<%s ?>" % self.__class__.__name__


class CreditLimit (AccountingEntity):
    
    """A limit on the charges a project can have.
    
    Attributes:
    start -- when this credit limit becomes active
    amount -- amount of credit authorized
    comment -- A verbose comment of why credit was allocated.
    project -- project that has the credit limit
    resource -- resource the credit limit is for
    
    Constraints:
    unique by project, resource, and start
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.start = kwargs.get("start")
        self.amount = kwargs.get("amount")
        self.comment = kwargs.get("comment")
        self.project = kwargs.get("project")
        self.resource = kwargs.get("resource")
    
    def _get_amount (self):
        return self._amount
    
    def _set_amount (self, value):
        if value < 0 and value is not None:
            raise ValueError("credit limit cannot be negative")
        self._amount = value
    
    amount = property(_get_amount, _set_amount)


class Request (AccountingEntity):
    
    """A request for amount on a resource.
    
    Attributes:
    datetime -- when the request was entered
    amount -- amount of amount requested
    comment -- verbose description of need
    start -- when the allocation should become active
    open -- the request remains unanswered
    resource -- the resource to be used
    project -- the project for which amount is requested
    allocations -- allocations on the system in response to this request
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.resource = kwargs.get("resource")
        self.project = kwargs.get("project")
        self.datetime = kwargs.get("datetime")
        self.amount = kwargs.get("amount")
        self.comment = kwargs.get("comment")
        self.start = kwargs.get("start")
        self.allocations = kwargs.get("allocations", [])
    
    def _get_amount (self):
        return self._amount
    
    def _set_amount (self, value):
        if value is not None:
            if value < 0:
                raise ValueError("cannot request negative amount")
        self._amount = value
    
    amount = property(_get_amount, _set_amount)
    
    def _get_allocated (self):
        """Whether the request has had amount allocated to it."""
        return len(self.allocations) > 0
    
    allocated = property(_get_allocated)
    
    def _get_open (self):
        """Whether the request is awaiting a reply."""
        return not self.allocated
    
    open = property(_get_open)


class Allocation (AccountingEntity):
    
    """An amount allocated to a project.
    
    Properties:
    request -- request for amount to which this is a response
    charges -- amount used from the allocation
    datetime -- when the allocation was entered
    approver -- the person/group who approved the allocation
    amount -- amount allocated
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
        self.approver = kwargs.get("approver")
        self.datetime = kwargs.get("datetime")
        self.amount = kwargs.get("amount")
        if self.amount is None and self.request:
            self.amount = self.request.amount
        self.start = kwargs.get("start")
        self.expiration = kwargs.get("expiration")
        self.comment = kwargs.get("comment")
        self.holds = kwargs.get("holds", [])
    
    def _get_project (self):
        """Return the related project."""
        return self.request.project
    
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.request.resource
    
    resource = property(_get_resource)
    
    def _get_amount (self):
        return self._amount
    
    def _set_amount (self, value):
        if value is not None:
            if value < 0:
                raise ValueError("cannot allocate negative amount")
        self._amount = value
    
    amount = property(_get_amount, _set_amount)
    
    def _get_started (self):
        """The allocation has a start date before now."""
        return self.start <= datetime.now()
    
    started = property(_get_started)
    
    def _get_expired (self):
        """The allocation has an expiration date before now."""
        return self.expiration <= datetime.now()
    
    expired = property(_get_expired)
    
    def _get_active (self):
        """The allocation's amount affect's the project's amount."""
        return self.started and not self.expired
    
    active = property(_get_active)
    
    def _get_charges (self):
        """Return the set of charges made against this allocation."""
        return Charge.query.join("hold").filter(Hold.allocation==self)
    
    charges = property(_get_charges)
    
    def _get_amount_charged (self):
        """Return the sum of effective charges against this allocation."""
        amount_charged = 0
        for charge in self.charges:
            amount_charged += charge.effective_charge
        return amount_charged
    
    amount_charged = property(_get_amount_charged)
    
    def _get_amount_held (self):
        """Sum of amount in open holds."""
        amount_held = 0
        for hold in self.holds:
            if hold.open:
                amount_held += hold.amount or 0
        return amount_held
    
    amount_held = property(_get_amount_held)
    
    def _get_amount_available (self):
        return self.amount - self.amount_held - self.amount_charged
    
    amount_available = property(_get_amount_available)


class Hold (AccountingEntity):
    
    """A potential charge against an allocation.
    
    Properties:
    datetime -- when the hold was entered
    amount -- how much could be charged
    comment -- verbose description of the hold
    project -- points to related project
    resource -- points to related resource
    effective_charge -- total amount charged (after refunds)
    amount_available -- difference of amount and effective_charge
    charged -- the hold has charges
    active -- the hold is against an active allocation
    open -- the hold is uncharged
    allocation -- the allocation the hold is against
    charges -- charges resulting from the hold
    
    Methods:
    charge -- charge amount from this hold
    
    Exceptions:
    InsufficientFunds -- hold exceed allocation
    """
    
    class InsufficientFunds (Exception):
        """Hold exceed allocations."""
    
    @classmethod
    def distributed (cls, allocations, **kwargs):
        
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
                holds[-1].amount += amount
            except IndexError:
                try:
                    allocation = allocations[0]
                except IndexError:
                    raise Exception("no allocations are available")
                hold = Hold(allocation=allocation, amount=amount, **kwargs)
                holds.append(hold)
        return holds
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.allocation = kwargs.get("allocation")
        self.datetime = kwargs.get("datetime")
        self.amount = kwargs.get("amount")
        self.comment = kwargs.get("comment")
        self.charges = kwargs.get("charges", [])
    
    def _get_amount (self):
        return self._amount
    
    def _set_amount (self, value):
        """Check that the value of the hold is valid."""
        if value is not None:
            if value < 0:
                raise ValueError("hold cannot be for negative amount")
            if getattr(self, "allocation", None) is not None:
                prev_value = getattr(self, "_amount", None)
                try:
                    self._amount = 0
                    credit_limit = self.allocation.project.credit_available(self.allocation.resource)
                    if value > self.allocation.amount - self.allocation.amount_held + credit_limit:
                        raise self.project.InsufficientFunds("credit limit exceeded")
                finally:
                    self._amount = prev_value
        self._amount = value
    
    amount = property(_get_amount, _set_amount)
    
    def _get_project (self):
        """Return the related project."""
        return self.allocation.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.allocation.resource
    resource = property(_get_resource)
    
    def _get_effective_charge (self):
        """Sum the effective charges of charges related to this hold."""
        effective_charge = 0
        for charge in self.charges:
            effective_charge += charge.effective_charge
        return effective_charge
    effective_charge = property(_get_effective_charge)
    
    def _get_amount_available (self):
        """Difference of amount held and effective charge."""
        return self.amount - self.effective_charge
    amount_available = property(_get_amount_available)
    
    def _get_charged (self):
        """The hold has been charged."""
        return len(self.charges) > 0
    charged = property(_get_charged)
    
    def _get_active (self):
        """The hold affects the current allocation."""
        return self.allocation.active
    active = property(_get_active)
    
    def _get_open (self):
        """The hold is still awaiting charges."""
        return not self.charged
    open = property(_get_open)


class Charge (AccountingEntity):
    
    """A charge against an allocation.
    
    Properties:
    project -- project from related request
    resource -- resource from related request
    datetime -- when the charge was deducted
    amount -- amount used
    comment -- a verbose description of the charge
    effective_charge -- The unit charge after any refunds
    active -- the charge is against an active hold
    hold -- the hold to which this charge applies
    refunds -- refunds against this charge
    
    Methods:
    refund -- refund amount from this charge
    """
    
    @classmethod
    def distributed (cls, holds, **kwargs):
        
        amount = kwargs.pop("amount")
        
        charges = list()
        for hold in holds:
            if hold.amount_available <= 0:
                continue
            
            if hold.amount_available >= amount:
                charge = Charge(hold=hold, amount=amount, **kwargs)
            else:
                charge = Charge(hold=hold, amount=hold.amount_available, **kwargs)
            charges.append(charge)
            amount -= charge.amount
        
        if amount > 0:
            try:
                charges[-1].amount += amount
            except IndexError:
                try:
                    hold = holds[0]
                except IndexError:
                    raise Exception("no holds are available to be charged")
                charge = Charge(hold=hold, amount=amount, **kwargs)
                charges.append(charge)
        return charges
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.hold = kwargs.get("hold")
        self.datetime = kwargs.get("datetime")
        self.amount = kwargs.get("amount")
        self.comment = kwargs.get("comment")
        self.refunds = kwargs.get("refunds", [])
    
    def _get_effective_charge (self):
        """Difference of charge amount and refund amounts."""
        effective_charge = self.amount or 0
        for refund in Refund.query.filter(Refund._charge==self):
            effective_charge -= refund.amount
        return effective_charge
    effective_charge = property(_get_effective_charge)
    
    def _get_project (self):
        """Return the related project."""
        return self.hold.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.hold.resource
    resource = property(_get_resource)
    
    def _get_amount (self):
        return self._amount
    
    def _set_amount (self, value):
        """Check that the values of the charge are valid."""
        if value is not None:
            if value < 0:
                raise ValueError("cannot charge negative amount")
        self._amount = value
    
    amount = property(_get_amount, _set_amount)
    
    def _get_active (self):
        """Charge affects the project's current allocation."""
        return self.hold.active
    active = property(_get_active)


class Refund (AccountingEntity):
    
    """A refund against a charge.
    
    Properties:
    project -- project from associated charge
    resource -- resource from associated charge
    charge -- charge being refunded
    datetime -- when the refund was added
    amount -- amount refunded
    comment -- description of the refund
    active -- refund is against an active charge
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.charge = kwargs.get("charge")
        self.datetime = kwargs.get("datetime")
        self.amount = kwargs.get("amount")
        self.comment = kwargs.get("comment")
    
    def _get_project (self):
        """Return the related project."""
        return self.charge.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.charge.resource
    resource = property(_get_resource)
    
    def _get_charge (self):
        return self._charge
    
    def _set_charge (self, charge):
        if charge is not None:
            if getattr(self, "amount", None) is not None:
                prev_charge = getattr(self, "_charge", None)
                try:
                    self._charge = None
                    if self.amount > charge.effective_charge:
                        raise ValueError("refunds cannot exceed charge")
                finally:
                    self._charge = prev_charge
        self._charge = charge
    
    charge = property(_get_charge, _set_charge)
    
    def _get_amount (self):
        return self._amount
    
    def _set_amount (self, value):
        if value is not None:
            if value < 0:
                raise ValueError("cannot refund negative amount")
            elif getattr(self, "charge", None) is not None:
                prev_value = getattr(self, "_amount", None)
                try:
                    self._amount = 0
                    if value > self.charge.effective_charge:
                        raise ValueError("refunds cannot exceed charge")
                finally:
                    self._amount = prev_value
        self._amount = value
    
    amount = property(_get_amount, _set_amount)
    
    def _get_active (self):
        """The charge affects the project's current allocation."""
        return self.charge.active
    active = property(_get_active)
