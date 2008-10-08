from nose.tools import raises

from datetime import datetime, timedelta

import clusterbank.model
import clusterbank.exceptions as exceptions
from clusterbank.model.entities import User, Project, Resource, \
    Request, Allocation, Hold, Charge, Refund, CreditLimit

__all__ = [
    "TestUser", "TestProject", "TestResource",
    "TestRequest", "TestAllocation",
    "TestCreditLimit",
    "TestHold", "TestCharge", "TestRefund",
]


class TestUser (object):
    
    def test_id (self):
        user = User(id=1)
        assert user.id == 1
    
    def test_name (self):
        user = User(id=1)
        assert user.name == "monty"


class TestProject (object):
    
    def test_id (self):
        project = Project(id=1)
        assert project.id == 1
    
    def test_name (self):
        project = Project(id=1)
        assert project.name == "grail"


class TestResource (object):
    
    def test_id (self):
        resource = Resource(id=1)
        assert resource.id == 1
    
    def test_name (self):
        resource = Resource(id=1)
        assert resource.name == "spam"


class TestRequest (object):
    
    def test_init (self):
        project = Project()
        resource = Resource()
        now = datetime.now()
        allocation = Allocation()
        request = Request(id=1, datetime=now, project=project,
            resource=resource, amount=2000, comment="testing", start=now,
            allocations=[allocation])
        assert request.id == 1
        assert request.datetime == now
        assert request.project is project
        assert request.resource is resource
        assert request.amount == 2000
        assert request.start == now
        assert request.comment == "testing"
        assert request.allocations == [allocation]


class TestAllocation (object):
    
    def test_init (self):
        now = datetime.now()
        project = Project()
        resource = Resource()
        request = Request()
        hold = Hold()
        charge = Charge(None, 0)
        allocation = Allocation(id=1, datetime=now, project=project,
            resource=resource, amount=1500, comment="testing", start=now,
            expiration=now+timedelta(days=1),
            requests=[request], holds=[hold], charges=[charge])
        assert allocation.id == 1
        assert allocation.datetime == now
        assert allocation.project is project
        assert allocation.resource is resource
        assert allocation.amount == 1500
        assert allocation.comment == "testing"
        assert allocation.start == now
        assert allocation.expiration == now + timedelta(days=1)
        assert allocation.requests == [request]
        assert allocation.holds == [hold]
        assert allocation.charges == [charge]
    
    def test_active (self):
        now = datetime.now()
        allocation = Allocation(start=now+timedelta(days=1), expiration=now+timedelta(days=2))
        assert not allocation.active
        allocation.start = now - timedelta(days=2)
        allocation.expiration = now - timedelta(days=1)
        assert not allocation.active
        allocation.expiration = now + timedelta(days=1)
        assert allocation.active
    
    def test_amount_available (self):
        allocation = Allocation(amount=1500)
        assert allocation.amount_available == 1500
        hold1 = Hold(allocation=allocation, amount=100)
        assert allocation.amount_available == 1400
        hold2 = Hold(allocation=allocation, amount=200)
        assert allocation.amount_available == 1200
        charge1 = Charge(allocation, 100)
        assert allocation.amount_available == 1100
        charge2 = Charge(allocation, 200)
        assert allocation.amount_available == 900
        hold1.active = False
        assert allocation.amount_available == 1000
        hold2.active = False
        assert allocation.amount_available == 1200
        refund1 = Refund(charge1, 50)
        assert allocation.amount_available == 1250
        refund2 = Refund(charge2, 100)
        assert allocation.amount_available == 1350


class TestCreditLimit (object):
    
    def test_init (self):
        now = datetime.now()
        project = Project()
        resource = Resource()
        credit_limit = CreditLimit(
            id=1, datetime=now,
            project=project, resource=resource,
            start=now, amount=100, comment="testing")
        assert credit_limit.id == 1
        assert credit_limit.datetime == now
        assert credit_limit.project is project
        assert credit_limit.resource is resource
        assert credit_limit.start == now
        assert credit_limit.amount == 100
        assert credit_limit.comment == "testing"


class TestHold (object):
    
    def test_init (self):
        now = datetime.now()
        user = User()
        allocation = Allocation()
        hold = Hold(
            id=1, datetime=now, active=False, user=user,
            allocation=allocation, amount=600, comment="testing")
        assert hold.id == 1
        assert hold.datetime == now
        assert hold.allocation is allocation
        assert hold.amount == 600
        assert hold.comment == "testing"
        assert hold.user is user
        assert not hold.active
    
    def test_default_active (self):
        hold = Hold()
        assert hold.active
    
    @raises(ValueError)
    def test_distributed_without_allocations (self):
        holds = Hold.distributed([], amount=900)
    
    def test_distributed (self):
        allocations = [Allocation(amount=600), Allocation(amount=600)]
        holds = Hold.distributed(allocations, amount=900)
        assert len(holds) == 2
        assert holds[0].allocation is allocations[0]
        assert holds[0].amount == 600
        assert holds[1].allocation is allocations[1]
        assert holds[1].amount == 300
    
    def test_distributed_zero_amount (self):
        allocations = [Allocation(amount=600), Allocation(amount=600)]
        holds = Hold.distributed(allocations, amount=0)
        assert len(holds) == 1, "holds: %i" % len(holds)
        hold = holds[0]
        assert hold.amount == 0, "hold: %i" % hold.amount
        assert hold.allocation in allocations, "allocation: %s" % hold.allocation


class TestCharge (object):
    
    def test_standard_init (self):
        now = datetime.now()
        allocation = Allocation()
        charge = Charge(allocation, 600)
        assert charge.id is None
        assert charge.datetime - now < timedelta(minutes=1)
        assert charge.allocation is allocation
        assert charge.amount == 600
        assert charge.comment is None
        assert charge.user is None
        assert charge.refunds == []
    
    @raises(ValueError)
    def test_distributed_without_allocations (self):
        charges = Charge.distributed([], amount=900)
    
    def test_distributed (self):
        allocations = [Allocation(amount=600), Allocation(amount=600)]
        charges = Charge.distributed(allocations, amount=900)
        assert len(charges) == 2
        assert sum(charge.amount for charge in charges) == 900
        assert charges[0].allocation is allocations[0]
        assert charges[0].amount == 600
        assert charges[1].allocation is allocations[1]
        assert charges[1].amount == 300
    
    def test_distributed_zero_amount (self):
        allocations = [Allocation(amount=600), Allocation(amount=600)]
        charges = Charge.distributed(allocations, amount=0)
        assert len(charges) == 1, "charges: %i" % len(charges)
        charge = charges[0]
        assert charge.amount == 0, "charge: %i" % charge.amount
        assert charge.allocation == allocations[0]
    
    def test_distributed_with_insufficient_allocation (self):
        allocations = [Allocation(amount=600), Allocation(amount=600)]
        charges = Charge.distributed(allocations, amount=1300)
        assert len(charges) == 2
        assert charges[0].allocation is allocations[0]
        assert charges[0].amount == 600
        assert charges[1].allocation is allocations[1]
        assert charges[1].amount == 700
    
    def test_amount_with_active_hold (self):
        allocation = Allocation(amount=1200)
        hold = Hold(allocation=allocation, amount=900, active=True)
        assert allocation.amount_available == 300
    
    def test_amount_with_inactive_hold (self):
        allocation = Allocation(amount=1200)
        hold = Hold(allocation=allocation, amount=allocation.amount, active=False)
        assert allocation.amount_available == 1200
    
    def test_amount_with_other_charge (self):
        allocation = Allocation(amount=1200)
        Charge(allocation, 600)
        Charge(allocation, 601)
        assert allocation.amount_available == -1
    
    def test_amount_with_refunded_charge (self):
        allocation = Allocation(amount=1200)
        charge = Charge(allocation, 600)
        Refund(charge, 600)
        Charge(allocation, 300)
        assert allocation.amount_available == 900, allocation.amount_available
    
    def test_amount_with_partially_refunded_charge (self):
        allocation = Allocation(amount=1200)
        charge = Charge(allocation, 600)
        Refund(charge, 400)
        Charge(allocation, 300)
        assert allocation.amount_available == 700
    
    def test_greater_amount_with_partially_refunded_charge (self):
        allocation = Allocation(amount=1200)
        charge = Charge(allocation, 1200)
        refund = Refund(charge, 600)
        Charge(allocation, 601)
        assert allocation.amount_available == -1
    
    def test_effective_amount (self):
        allocation = Allocation()
        charge = Charge(allocation, 100)
        assert charge.effective_amount == 100
        refund1 = Refund(charge, 10)
        assert charge.effective_amount == 90
        refund2 = Refund(charge, 20)
        assert charge.effective_amount == 70


class TestRefund (object):
    
    def test_init (self):
        now = datetime.now()
        charge = Charge(None, 0)
        refund = Refund(charge, 300)
        assert refund.id is None
        assert refund.datetime - now < timedelta(minutes=1)
        assert refund.charge is charge
        assert refund.amount == 300
        assert refund.comment is None
    
    def test_full_refund (self):
        charge = Charge(None, 100)
        refund = Refund(charge)
        assert refund.amount == 100

