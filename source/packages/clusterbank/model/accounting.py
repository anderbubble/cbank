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

from sqlalchemy import desc, and_

__all__ = ["Request", "Allocation", "CreditLimit", "Hold", "Charge", "Refund"]


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


class Request (AccountingEntity):
    
    """A request for amount on a resource."""
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.project = kwargs.get("project")
        self.resource = kwargs.get("resource")
        self.datetime = kwargs.get("datetime")
        self.amount = kwargs.get("amount")
        self.start = kwargs.get("start")
        self.comment = kwargs.get("comment")
        self.allocation = kwargs.get("allocation")
    
    def _get_amount (self):
        return self._amount
    
    def _set_amount (self, value):
        if value is not None:
            if value < 0:
                raise ValueError("cannot request negative amount")
        self._amount = value
    
    amount = property(_get_amount, _set_amount)


class Allocation (AccountingEntity):
    
    """An amount allocated to a project."""
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.project = kwargs.get("project")
        self.resource = kwargs.get("resource")
        self.datetime = kwargs.get("datetime")
        self.amount = kwargs.get("amount")
        self.start = kwargs.get("start")
        self.expiration = kwargs.get("expiration")
        self.comment = kwargs.get("comment")
        self.holds = kwargs.get("holds", [])
        self.charges = kwargs.get("charges", [])
    
    def _get_amount (self):
        return self._amount
    
    def _set_amount (self, value):
        if value is not None:
            if value < 0:
                raise ValueError("cannot allocate negative amount")
        self._amount = value
    
    amount = property(_get_amount, _set_amount)
    
    def _get_amount_available (self):
        charges = Charge.query.filter(Charge.allocation==self)
        amount_charged = (charges.sum("amount") or 0) - (charges.join("refunds").sum("amount") or 0)
        return self.amount - amount_charged
    
    amount_available = property(_get_amount_available)
    
    def _get_active (self):
        """The allocation's amount affect's the project's amount."""
        return self.start <= datetime.now() < self.expiration
    
    active = property(_get_active)


class CreditLimit (AccountingEntity):
    
    """A limit on the charges a project can have.
    
    Constraints:
    unique by project, resource, and start
    """
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.project = kwargs.get("project")
        self.resource = kwargs.get("resource")
        self.datetime = kwargs.get("datetime")
        self.start = kwargs.get("start")
        self.amount = kwargs.get("amount")
        self.comment = kwargs.get("comment")
    
    def _get_amount (self):
        return self._amount
    
    def _set_amount (self, value):
        if value < 0 and value is not None:
            raise ValueError("credit limit cannot be negative")
        self._amount = value
    
    amount = property(_get_amount, _set_amount)


class Hold (AccountingEntity):
    
    """A potential charge against an allocation."""
    
    class InsufficientFunds (Exception):
        """Hold exceed allocations."""
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.allocation = kwargs.get("allocation")
        self.datetime = kwargs.get("datetime")
        self.amount = kwargs.get("amount")
        self.comment = kwargs.get("comment")
    
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
                    try:
                        credit_limit = CreditLimit.query.filter(and_(CreditLimit.project==self.project, CreditLimit.resource==self.resource)).filter(CreditLimit.start<=datetime.now()).order_by(desc("start"))[0].amount
                    except IndexError:
                        credit_limit = 0
                    if value > self.allocation.amount - Hold.query.filter(Hold.allocation==self.allocation).sum("amount") + credit_limit:
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


class Charge (AccountingEntity):
    
    """A charge against an allocation."""
    
    def __init__ (self, **kwargs):
        self.id = kwargs.get("id")
        self.allocation = kwargs.get("allocation")
        self.amount = kwargs.get("amount")
        if kwargs.get("hold") is not None:
            hold = kwargs.get("hold")
            hold.active = False
            if self.allocation is None:
                self.allocation = hold.allocation
            if self.amount is None:
                self.amount = hold.amount
        self.datetime = kwargs.get("datetime")
        self.comment = kwargs.get("comment")
        self.refunds = kwargs.get("refunds", [])
    
    @classmethod
    def distributed (cls, allocations, **kwargs):
        
        amount = kwargs.pop("amount")
        
        charges = list()
        for allocation in allocations:
            if allocation.amount_available <= 0:
                continue
            
            if allocation.amount_available >= amount:
                charge = Charge(allocation=allocation, amount=amount, **kwargs)
            else:
                charge = Charge(allocation=allocation, amount=allocation.amount_available, **kwargs)
            charges.append(charge)
            amount -= charge.amount
        
        if amount > 0:
            try:
                charges[-1].amount += amount
            except IndexError:
                try:
                    allocation = allocations[0]
                except IndexError:
                    raise Exception("no allocations are available to be charged")
                charge = Charge(allocation=allocation, amount=amount, **kwargs)
                charges.append(charge)
        return charges
    
    def _get_amount (self):
        return self._amount
    
    def _set_amount (self, value):
        """Check that the values of the charge are valid."""
        if value is not None:
            if value < 0:
                raise ValueError("cannot charge negative amount")
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
    
    def _get_effective_amount (self):
        return self.amount - (Refund.query.filter(Refund._charge==self).sum("amount") or 0)
    
    effective_amount = property(_get_effective_amount)


class Refund (AccountingEntity):
    
    """A refund against a charge."""
    
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
    
    def _get_amount (self):
        return self._amount
    
    def _set_amount (self, value):
        if value is not None:
            if value < 0:
                raise ValueError("cannot refund negative amount")
            prev_value = getattr(self, "_amount", None)
            try:
                self._amount = 0
                if value > self.charge.effective_amount:
                    raise ValueError("refunds cannot exceed charge")
            finally:
                self._amount = prev_value
        self._amount = value
    
    amount = property(_get_amount, _set_amount)
