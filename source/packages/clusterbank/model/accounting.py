"""Cluster accounting model.

Classes:
Request -- request for amount on a resource
Allocation -- record of amount allocated to a project
CreditLimit -- a maximum negative value for a project on a resource
Hold -- a potential charge against a allocation
Charge -- charge against a allocation
Refund -- refund of a charge
"""

from datetime import datetime

import sqlalchemy.orm.session
from sqlalchemy import desc
try:
    set
except NameError:
    from sets import Set as set

__all__ = ["Request", "Allocation", "CreditLimit", "Hold", "Charge", "Refund"]


class AccountingEntity (object):
    """Base class for accounting entities.
    
    Provides a standard str/repr interface.
    """
    
    def __str__ (self):
        if self.id is None:
            id = "?"
        else:
            id = self.id
        if self.amount is None:
            amount = "?"
        else:
            amount = self.amount
        return "%s (%s)" % (id, amount)
    
    def __repr__ (self):
        return "<%s %r>" % (self.__class__.__name__, self.id)


class Request (AccountingEntity):
    
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
    
    class SessionExtension (sqlalchemy.orm.session.SessionExtension):
        
        def forbid_negative_amounts (self, session):
            requests = [instance for instance in (session.new | session.dirty) if isinstance(instance, Request)]
            for request in requests:
                if request.amount < 0:
                    raise ValueError("cannot request negative amount")
        
        def before_commit (self, session):
            self.forbid_negative_amounts(session)
    
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


class Allocation (AccountingEntity):
    
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
    
    class SessionExtension (sqlalchemy.orm.session.SessionExtension):
        
        def forbid_negative_amounts (self, session):
            allocations = [instance for instance in (session.new | session.dirty) if isinstance(instance, Allocation)]
            for allocation in allocations:
                if allocation.amount < 0:
                    raise ValueError("cannot allocate negative amount")
        
        def before_commit (self, session):
            self.forbid_negative_amounts(session)
    
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
    
    def _get_amount_available (self):
        """Intelligent property accessor."""
        # sums are typecast to integers because mysql returns strings when summing integers
        amount_charged = int(Charge.query.filter(Charge.allocation==self).sum(Charge.amount) or 0)
        amount_refunded = int(Refund.query.join("charge").filter(Charge.allocation==self).sum(Refund.amount) or 0)
        amount_held = int(Hold.query.filter(Hold.allocation==self).filter(Hold.active==True).sum(Hold.amount) or 0)
        return self.amount - ((amount_charged - amount_refunded) + amount_held)
    
    amount_available = property(_get_amount_available)
    
    def _get_active (self):
        """Intelligent property accessor."""
        return self.start <= datetime.now() < self.expiration
    
    active = property(_get_active)


class CreditLimit (AccountingEntity):
    
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
    
    class SessionExtension (sqlalchemy.orm.session.SessionExtension):
        
        def forbid_negative_amounts (self, session):
            credit_limits = [instance for instance in (session.new | session.dirty) if isinstance(instance, CreditLimit)]
            for credit_limit in credit_limits:
                if credit_limit.amount < 0:
                    raise ValueError("credit limit cannot be negative")
        
        def before_commit (self, session):
            self.forbid_negative_amounts(session)
    
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


class Hold (AccountingEntity):
    
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
    
    class SessionExtension (sqlalchemy.orm.session.SessionExtension):
        
        def forbid_negative_amounts (self, session):
            holds = [instance for instance in (session.new | session.dirty) if isinstance(instance, Hold)]
            for hold in holds:
                if hold.amount < 0:
                    raise ValueError("hold cannot be for negative amount")
        
        def forbid_hold_greater_than_allocation (self, session):
            holds = [instance for instance in (session.new | session.dirty) if isinstance(instance, Hold)]
            for allocation in set([hold.allocation for hold in holds]):
                credit_limit = allocation.project.credit_limit(allocation.resource)
                if credit_limit:
                    credit_limit = credit_limit.amount
                else:
                    credit_limit = 0
                if allocation.amount_available < -credit_limit:
                    raise allocation.project.InsufficientFunds("not enough funds available")
        
        def before_commit (self, session):
            self.forbid_negative_amounts(session)
            self.forbid_hold_greater_than_allocation(session)
    
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


class Charge (AccountingEntity):
    
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
    
    class SessionExtension (sqlalchemy.orm.session.SessionExtension):
        
        def forbid_negative_amount (self, session):
            charges = [instance for instance in (session.new | session.dirty) if isinstance(instance, Charge)]
            for charge in charges:
                if charge.amount < 0:
                    raise ValueError("charge cannot be for negative amount")
        
        def before_commit (self, session):
            self.forbid_negative_amount(session)
    
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
        """Intelligent property accessor."""
        # sums are typecast to integers because mysql returns strings when summing integers
        amount_refunded = int(Refund.query.filter_by(charge=self).sum(Refund.amount) or 0)
        return self.amount - amount_refunded
    
    effective_amount = property(_get_effective_amount)


class Refund (AccountingEntity):
    
    """A refund against a charge.
    
    Properties:
    id -- unique integer identifier
    charge -- charge being refunded
    datetime -- when the charge was entered
    amount -- amount refunded
    comment -- misc. comments
    """
    
    class SessionExtension (sqlalchemy.orm.session.SessionExtension):
        
        def forbid_negative_amount (self, session):
            refunds = [instance for instance in (session.new | session.dirty) if isinstance(instance, Refund)]
            for refund in refunds:
                if refund.amount < 0:
                    raise ValueError("cannot refund negative amount")
        
        def forbid_refund_greater_than_charge (self, session):
            refunds = [instance for instance in (session.new | session.dirty) if isinstance(instance, Refund)]
            charges = set([refund.charge for refund in refunds])
            for charge in charges:
                if charge.effective_amount < 0:
                    raise ValueError("refunds cannot exceed charge")
        
        def before_commit (self, session):
            self.forbid_negative_amount(session)
            self.forbid_refund_greater_than_charge(session)
    
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
            self.amount = self.charge.amount
