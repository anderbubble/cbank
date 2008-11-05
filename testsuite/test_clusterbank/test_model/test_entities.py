from nose.tools import raises

from datetime import datetime, timedelta

import clusterbank.model
import clusterbank.exceptions as exceptions
from clusterbank.model.entities import User, Project, Resource, \
    Allocation, Hold, Job, Charge, Refund

__all__ = [
    "TestUser", "TestProject", "TestResource",
    "TestAllocation", "TestHold", "TestCharge", "TestRefund",
]


def assert_in (item, container):
    assert item in container, "%s not in %s" % (item, container)


class TestUser (object):
    
    def test_id (self):
        user = User(1)
        assert user.id == 1
    
    def test_name (self):
        user = User(1)
        assert user.name == "monty"


class TestProject (object):
    
    def test_id (self):
        project = Project(1)
        assert project.id == 1
    
    def test_name (self):
        project = Project(1)
        assert project.name == "grail"


class TestResource (object):
    
    def test_id (self):
        resource = Resource(1)
        assert resource.id == 1
    
    def test_name (self):
        resource = Resource(1)
        assert resource.name == "spam"


class TestAllocation (object):
    
    def test_init (self):
        start = datetime.now()
        project = Project(1)
        resource = Resource(1)
        allocation = Allocation(project, resource, 1500,
            start, start+timedelta(days=1))
        assert allocation.id is None
        assert datetime.now() - allocation.datetime < timedelta(minutes=1)
        assert allocation.project is project
        assert allocation.resource is resource
        assert allocation.amount == 1500
        assert allocation.comment is None
        assert allocation.start == start
        assert allocation.expiration == start + timedelta(days=1)
        assert allocation.holds == []
        assert allocation.charges == []
    
    def test_active (self):
        now = datetime.now()
        allocation = Allocation(None, None, 0,
            now+timedelta(days=1), now+timedelta(days=2))
        assert not allocation.active
        allocation.start = now - timedelta(days=2)
        allocation.expiration = now - timedelta(days=1)
        assert not allocation.active
        allocation.expiration = now + timedelta(days=1)
        assert allocation.active
    
    def test_amount_with_active_hold (self):
        allocation = Allocation(None, None, 1200, None, None)
        hold = Hold(allocation, 900)
        assert allocation.amount_available() == 300
    
    def test_amount_with_inactive_hold (self):
        allocation = Allocation(None, None, 1200, None, None)
        hold = Hold(allocation, allocation.amount)
        hold.active = False
        assert allocation.amount_available() == 1200
    
    def test_amount_with_other_charge (self):
        allocation = Allocation(None, None, 1200, None, None)
        Charge(allocation, 600)
        Charge(allocation, 601)
        assert allocation.amount_available() == -1
    
    def test_amount_with_refunded_charge (self):
        allocation = Allocation(None, None, 1200, None, None)
        charge = Charge(allocation, 600)
        Refund(charge, 600)
        Charge(allocation, 300)
        assert allocation.amount_available() == 900, \
            allocation.amount_available()
    
    def test_amount_with_partially_refunded_charge (self):
        allocation = Allocation(None, None, 1200, None, None)
        charge = Charge(allocation, 600)
        Refund(charge, 400)
        Charge(allocation, 300)
        assert allocation.amount_available() == 700
    
    def test_greater_amount_with_partially_refunded_charge (self):
        allocation = Allocation(None, None, 1200, None, None)
        charge = Charge(allocation, 1200)
        refund = Refund(charge, 600)
        Charge(allocation, 601)
        assert allocation.amount_available() == -1
    
    def test_amount_available (self):
        allocation = Allocation(None, None, 1500, None, None)
        assert allocation.amount_available() == 1500
        hold1 = Hold(allocation, 100)
        assert allocation.amount_available() == 1400
        hold2 = Hold(allocation, 200)
        assert allocation.amount_available() == 1200
        charge1 = Charge(allocation, 100)
        assert allocation.amount_available() == 1100
        charge2 = Charge(allocation, 200)
        assert allocation.amount_available() == 900
        hold1.active = False
        assert allocation.amount_available() == 1000
        hold2.active = False
        assert allocation.amount_available() == 1200
        refund1 = Refund(charge1, 50)
        assert allocation.amount_available() == 1250
        refund2 = Refund(charge2, 100)
        assert allocation.amount_available() == 1350


class TestHold (object):
    
    def test_init (self):
        allocation = Allocation(None, None, 0, None, None)
        hold = Hold(allocation, 600)
        assert hold.id is None
        assert datetime.now() - hold.datetime < timedelta(minutes=1)
        assert hold.allocation is allocation
        assert hold.amount == 600
        assert hold.comment is None
        assert hold.user is None
        assert hold.active
    
    @raises(ValueError)
    def test_distributed_without_allocations (self):
        holds = Hold.distributed([], amount=900)
    
    def test_distributed (self):
        start = datetime.now()
        expiration = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, expiration),
            Allocation(None, None, 600, start, expiration)]
        holds = Hold.distributed(allocations, amount=900)
        assert len(holds) == 2
        assert holds[0].allocation is allocations[0]
        assert holds[0].amount == 600
        assert holds[1].allocation is allocations[1]
        assert holds[1].amount == 300
    
    def test_distributed_zero_amount (self):
        start = datetime.now()
        expiration = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, expiration),
            Allocation(None, None, 600, start, expiration)]
        holds = Hold.distributed(allocations, amount=0)
        assert len(holds) == 1, "holds: %i" % len(holds)
        hold = holds[0]
        assert hold.amount == 0, "hold: %i" % hold.amount
        assert_in(hold.allocation, allocations)


class TestJob (object):
    
    def test_init (self):
        job = Job("www.example.com.123")
        assert job.id == "www.example.com.123"
        assert job.user is None
        assert job.group is None
        assert job.account is None
        assert job.name is None
        assert job.queue is None
        assert job.reservation_name is None
        assert job.reservation_id is None
        assert job.ctime is None
        assert job.qtime is None
        assert job.etime is None
        assert job.start is None
        assert job.exec_host is None
        assert job.resource_list == {}
        assert job.session is None
        assert job.alternate_id is None
        assert job.end is None
        assert job.exit_status is None
        assert job.resources_used == {}
        assert job.accounting_id is None
        assert job.charges == [] # cbank specific
    

class TestCharge (object):
    
    def test_init (self):
        now = datetime.now()
        allocation = Allocation(None, None, 0, None, None)
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
        start = datetime.now()
        expiration = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, expiration),
            Allocation(None, None, 600, start, expiration)]
        charges = Charge.distributed(allocations, amount=900)
        assert len(charges) == 2
        assert sum(charge.amount for charge in charges) == 900
        assert charges[0].allocation is allocations[0]
        assert charges[0].amount == 600
        assert charges[1].allocation is allocations[1]
        assert charges[1].amount == 300
    
    def test_distributed_zero_amount (self):
        start = datetime.now()
        expiration = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, expiration),
            Allocation(None, None, 600, start, expiration)]
        charges = Charge.distributed(allocations, amount=0)
        assert len(charges) == 1, "charges: %i" % len(charges)
        charge = charges[0]
        assert charge.amount == 0, "charge: %i" % charge.amount
        assert charge.allocation == allocations[0]
    
    def test_distributed_with_insufficient_allocation (self):
        start = datetime.now()
        expiration = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, expiration),
            Allocation(None, None, 600, start, expiration)]
        charges = Charge.distributed(allocations, amount=1300)
        assert len(charges) == 2
        assert charges[0].allocation is allocations[0]
        assert charges[0].amount == 600
        assert charges[1].allocation is allocations[1]
        assert charges[1].amount == 700
    
    def test_effective_amount (self):
        charge = Charge(None, 100)
        assert charge.effective_amount() == 100
        Refund(charge, 10)
        assert charge.effective_amount() == 90
        Refund(charge, 20)
        assert charge.effective_amount() == 70


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

