from datetime import datetime, timedelta

from nose.tools import raises

import clusterbank.model
import clusterbank.exceptions as exceptions
from clusterbank.model.entities import Project, Resource
from clusterbank.model.accounting import \
    Request, Allocation, CreditLimit, Hold, Charge, Refund

class AccountingTester (object):
    
    def setup (self):
        """Create the tables before each test."""
        clusterbank.model.metadata.create_all()
        self.now = datetime.now()
        self.project = Project.by_name("grail")
        self.resource = Resource.by_name("spam")
    
    def teardown (self):
        """drop the database after each test."""
        clusterbank.model.Session.close()
        clusterbank.model.metadata.drop_all()


class TestRequest (AccountingTester):
    
    def test_init (self):
        request = Request(
            id=1, datetime=self.now,
            project=self.project, resource=self.resource,
            amount=2000, comment="testing", start=self.now)
        assert request.id == 1
        assert request.datetime == self.now
        assert request.project is self.project
        assert request.resource is self.resource
        assert request.amount == 2000
        assert request.start == self.now
        assert request.comment == "testing"
        assert not request.allocations
    
    def test_automatic_id (self):
        request = Request(
            project=self.project, resource=self.resource, amount=2000)
        clusterbank.model.Session.commit()
        assert request.id is not None
    
    def test_automatic_datetime (self):
        request = Request(
            project=self.project, resource=self.resource, amount=2000)
        clusterbank.model.Session.commit()
        assert request.datetime - datetime.now() < timedelta(minutes=1)
    
    @raises(ValueError)
    def test_negative_amount (self):
        request = Request()
        request.amount = -100
        clusterbank.model.Session.commit()
    
    def test_allocations (self):
        request = Request(
            project=self.project, resource=self.resource, amount=2000)
        allocation = Allocation(amount=request.amount,
            project=request.project, resource=request.resource,
            start=self.now, expiration=self.now+timedelta(days=1),
            requests=[request])
        clusterbank.model.Session.commit()
        assert set(request.allocations) == set([allocation])


class TestAllocation (AccountingTester):
    
    def test_init (self):
        allocation = Allocation(
            id=1, datetime=self.now,
            project=self.project, resource=self.resource,
            amount=1500, comment="testing",
            start=self.now, expiration=self.now+timedelta(days=1))
        assert allocation.id == 1
        assert allocation.datetime == self.now
        assert allocation.project is self.project
        assert allocation.resource is self.resource
        assert allocation.amount == 1500
        assert allocation.comment == "testing"
        assert allocation.start == self.now
        assert allocation.expiration == self.now + timedelta(days=1)
        assert not allocation.holds
        assert not allocation.charges
    
    def test_automatic_id (self):
        allocation = Allocation(amount=1500,
            project=self.project, resource=self.resource,
            start=self.now, expiration=self.now+timedelta(days=1))
        clusterbank.model.Session.commit()
        assert allocation.id is not None
    
    def test_automatic_datetime (self):
        allocation = Allocation(amount=1500,
            project=self.project, resource=self.resource,
            start=self.now, expiration=self.now+timedelta(days=1))
        clusterbank.model.Session.commit()
        assert allocation.datetime - datetime.now() < timedelta(minutes=1)
    
    def test_requests (self):
        request = Request(
            project=self.project, resource=self.resource, amount=2000)
        allocation = Allocation(amount=1500,
            project=self.project, resource=self.resource,
            start=self.now+timedelta(days=1), expiration=self.now+timedelta(days=2))
        request.allocations = [allocation]
        clusterbank.model.Session.commit()
        assert set(allocation.requests) == set([request])
    
    def test_holds (self):
        allocation = Allocation(amount=1500,
            project=self.project, resource=self.resource,
            start=self.now, expiration=self.now+timedelta(days=1))
        clusterbank.model.Session.flush() # SA <= 0.4.3 has transient query bug
        hold = Hold(allocation=allocation, amount=100)
        clusterbank.model.Session.commit()
        assert set(allocation.holds) == set([hold])
        
    def test_charges (self):
        allocation = Allocation(amount=1500,
            project=self.project, resource=self.resource,
            start=self.now, expiration=self.now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        clusterbank.model.Session.commit()
        assert set(allocation.charges) == set([charge])
    
    def test_active (self):
        allocation = Allocation(amount=1500,
            project=self.project, resource=self.resource,
            start=self.now+timedelta(days=1), expiration=self.now+timedelta(days=2))
        assert not allocation.active
        allocation.start = self.now - timedelta(days=2)
        allocation.expiration = self.now - timedelta(days=1)
        assert not allocation.active
        allocation.expiration = self.now + timedelta(days=1)
        assert allocation.active
    
    @raises(ValueError)
    def test_negative_amount (self):
        allocation = Allocation()
        allocation.amount = -100
        clusterbank.model.Session.commit()
    
    def test_amount_available (self):
        allocation = Allocation(amount=1500,
            project=self.project, resource=self.resource,
            start=self.now, expiration=self.now+timedelta(days=1))
        clusterbank.model.Session.flush() # SA <= 0.4.3 has transient query bug
        assert allocation.amount_available == 1500
        hold1 = Hold(allocation=allocation, amount=100)
        assert allocation.amount_available == 1400
        hold2 = Hold(allocation=allocation, amount=200)
        assert allocation.amount_available == 1200
        charge1 = Charge(allocation=allocation, amount=100)
        assert allocation.amount_available == 1100
        charge2 = Charge(allocation=allocation, amount=200)
        assert allocation.amount_available == 900
        hold1.active = False
        assert allocation.amount_available == 1000
        hold2.active = False
        assert allocation.amount_available == 1200
        refund1 = Refund(charge=charge1, amount=50)
        assert allocation.amount_available == 1250
        refund2 = Refund(charge=charge2, amount=100)
        assert allocation.amount_available == 1350


class TestCreditLimit (AccountingTester):
    
    def test_init (self):
        credit_limit = CreditLimit(
            id=1, datetime=self.now,
            project=self.project, resource=self.resource,
            start=self.now, amount=100, comment="testing")
        assert credit_limit.id == 1
        assert credit_limit.datetime == self.now
        assert credit_limit.project is self.project
        assert credit_limit.resource is self.resource
        assert credit_limit.start == self.now
        assert credit_limit.amount == 100
        assert credit_limit.comment == "testing"
    
    def test_automatic_id (self):
        credit_limit = CreditLimit(
            project=self.project, resource=self.resource,
            start=self.now, amount=100)
        clusterbank.model.Session.commit()
        assert credit_limit.id is not None
    
    def test_automatic_datetime (self):
        credit_limit = CreditLimit(
            project=self.project, resource=self.resource,
            start=self.now, amount=100)
        clusterbank.model.Session.commit()
        assert credit_limit.datetime - datetime.now() < timedelta(minutes=1)
    
    @raises(ValueError)
    def test_negative (self):
        credit_limit = CreditLimit(
            project=self.project, resource=self.resource,
            start=self.now, amount=-100)
        clusterbank.model.Session.commit()


class TestHold (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.allocation = Allocation(amount=1200,
            project=self.project, resource=self.resource,
            start=self.now, expiration=self.now+timedelta(days=1))
        clusterbank.model.Session.flush() # SA <= 0.4.3 has transient query bug
    
    def test_init (self):
        hold = Hold(
            id=1, datetime=self.now, active=False,
            allocation=self.allocation, amount=600, comment="testing")
        assert hold.id == 1
        assert hold.datetime == self.now
        assert hold.allocation is self.allocation
        assert hold.amount == 600
        assert hold.comment == "testing"
        assert not hold.active
    
    def test_automatic_id (self):
        hold = Hold(allocation=self.allocation, amount=600)
        clusterbank.model.Session.commit()
        assert hold.id is not None
    
    def test_automatic_datetime (self):
        hold = Hold(allocation=self.allocation, amount=600)
        clusterbank.model.Session.commit()
        assert hold.datetime - datetime.now() < timedelta(minutes=1)
    
    def test_default_active (self):
        hold = Hold(allocation=self.allocation, amount=600)
        clusterbank.model.Session.commit()
        assert hold.active
    
    @raises(Exception)
    def test_distributed_without_allocations (self):
        holds = Hold.distributed([], amount=900)
    
    def test_distributed (self):
        allocation1 = self.allocation
        allocation1.amount = 600
        allocation2 = Allocation(amount=600,
            project=allocation1.project, resource=allocation1.resource,
            start=allocation1.start, expiration=allocation1.start)
        holds = Hold.distributed((allocation1, allocation2), amount=900)
        assert len(holds) == 2
        assert sum(hold.amount for hold in holds) == 900
        for hold in holds:
            assert hold.allocation in (allocation1, allocation2)
    
    @raises(ValueError)
    def test_negative_amount (self):
        hold = Hold()
        hold.amount = -100
        clusterbank.model.Session.commit()
    
    @raises(exceptions.InsufficientFunds)
    def test_excessive_amount (self):
        hold = Hold(allocation=self.allocation)
        hold.amount = self.allocation.amount + 1
        clusterbank.model.Session.commit()
    
    @raises(exceptions.InsufficientFunds)
    def test_amount_with_active_hold (self):
        hold1 = Hold(allocation=self.allocation, amount=self.allocation.amount)
        hold2 = Hold(allocation=hold1.allocation)
        hold2.amount = 1
        clusterbank.model.Session.commit()
    
    def test_amount_with_inactive_hold (self):
        hold1 = Hold(allocation=self.allocation, amount=self.allocation.amount, active=False)
        hold2 = Hold(allocation=hold1.allocation, amount=hold1.allocation.amount)
        try:
            clusterbank.model.Session.commit()
        except exceptions.InsufficientFunds:
            assert False, "didn't correctly deactivate hold"
    
    @raises(exceptions.InsufficientFunds)
    def test_amount_with_charge (self):
        charge = Charge(allocation=self.allocation, amount=self.allocation.amount)
        hold = Hold(allocation=charge.allocation)
        hold.amount = 1
        clusterbank.model.Session.commit()
    
    def test_amount_with_refunded_charge (self):
        charge = Charge(allocation=self.allocation, amount=self.allocation.amount)
        refund = Refund(charge=charge, amount=charge.amount)
        hold = Hold(allocation=charge.allocation)
        hold.amount = charge.allocation.amount
        try:
            clusterbank.model.Session.commit()
        except self.allocation.project.InsufficientFunds:
            assert False, "Didn't correctly refund charge."
    
    def test_amount_with_partially_refunded_charge (self):
        charge = Charge(allocation=self.allocation, amount=self.allocation.amount)
        refund = Refund(charge=charge, amount=charge.amount//2)
        hold = Hold(allocation=charge.allocation)
        hold.amount = charge.amount - refund.amount
        try:
            clusterbank.model.Session.commit()
        except exceptions.InsufficientFunds:
            assert False, "Didn't correctly refund charge."
    
    @raises(exceptions.InsufficientFunds)
    def test_greater_amount_after_refunded_charge (self):
        charge = Charge(allocation=self.allocation, amount=self.allocation.amount)
        refund = Refund(charge=charge, amount=charge.amount//2)
        hold = Hold(allocation=charge.allocation)
        hold.amount = (charge.amount - refund.amount) + 1
        clusterbank.model.Session.commit()


class TestCharge (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.allocation = Allocation(amount=1200,
            project=self.project, resource=self.resource,
            start=self.now, expiration=self.now+timedelta(days=1))
        clusterbank.model.Session.flush() # SA <= 0.4.3 has transient query bug
    
    def test_standard_init (self):
        charge = Charge(
            id=1, datetime=self.now,
            allocation=self.allocation, amount=600, comment="testing")
        assert charge.id == 1
        assert charge.datetime == self.now
        assert charge.allocation is self.allocation
        assert charge.amount == 600
        assert charge.comment == "testing"
    
    def test_hold_init (self):
        hold = Hold(allocation=self.allocation, amount=900, comment="testing")
        charge = Charge(hold=hold)
        assert not hold.active
        assert charge.allocation is hold.allocation
        assert charge.amount == hold.amount
    
    def test_automatic_id (self):
        charge = Charge(allocation=self.allocation, amount=600)
        clusterbank.model.Session.commit()
        assert charge.id is not None
    
    def test_automatic_datetime (self):
        charge = Charge(allocation=self.allocation, amount=600)
        clusterbank.model.Session.commit()
        assert charge.datetime - datetime.now() < timedelta(minutes=1)
    
    @raises(ValueError)
    def test_distributed_without_allocations (self):
        charges = Charge.distributed([], amount=900)
        clusterbank.model.Session.commit()
    
    def test_distributed (self):
        allocation1 = self.allocation
        allocation1.amount = 600
        allocation2 = Allocation(amount=600,
            project=allocation1.project, resource=allocation1.resource,
            start=allocation1.start, expiration=allocation1.start)
        charges = Charge.distributed((allocation1, allocation2), amount=900)
        clusterbank.model.Session.commit()
        assert len(charges) == 2
        assert sum(charge.amount for charge in charges) == 900
        for charge in charges:
            assert charge.allocation in (allocation1, allocation2)
    
    def test_distributed_with_insufficient_allocation (self):
        allocation1 = self.allocation
        allocation1.amount = 600
        allocation2 = Allocation(amount=600,
            project=allocation1.project, resource=allocation1.resource,
            start=allocation1.start, expiration=allocation1.start)
        charges = Charge.distributed((allocation1, allocation2), amount=1300)
        clusterbank.model.Session.commit()
        assert len(charges) == 2
        assert sum(charge.amount for charge in charges) == 1300
        for charge in charges:
            assert charge.allocation in (allocation1, allocation2)
    
    @raises(ValueError)
    def test_negative_amount (self):
        charge = Charge()
        charge.amount = -100
        clusterbank.model.Session.commit()
    
    @raises(exceptions.InsufficientFunds)
    def test_amount_with_active_hold (self):
        hold = Hold(allocation=self.allocation, amount=self.allocation.amount)
        charge = Charge(allocation=hold.allocation, amount=1)
        clusterbank.model.Session.commit()
    
    def test_amount_with_inactive_hold (self):
        hold = Hold(allocation=self.allocation, amount=self.allocation.amount, active=False)
        charge = Charge(allocation=hold.allocation, amount=hold.allocation.amount)
        try:
            clusterbank.model.Session.commit()
        except exceptions.InsufficientFunds:
            assert False, "Didn't correctly deactivate other hold."
    
    def test_amount_with_other_charge (self):
        charge1 = Charge(allocation=self.allocation, amount=self.allocation.amount)
        charge2 = Charge(allocation=charge1.allocation, amount=1)
        clusterbank.model.Session.commit()
        assert self.allocation.amount_available == -1
    
    def test_amount_with_refunded_charge (self):
        charge1 = Charge(allocation=self.allocation, amount=self.allocation.amount)
        refund = Refund(charge=charge1, amount=charge1.amount)
        charge2 = Charge(allocation=charge1.allocation, amount=charge1.allocation.amount)
        clusterbank.model.Session.flush() # SA <= 0.4.3 has transient query bug
        try:
            clusterbank.model.Session.commit()
        except exceptions.InsufficientFunds:
            assert False, "Didn't correctly refund charge."
    
    def test_amount_with_partially_refunded_charge (self):
        charge1 = Charge(allocation=self.allocation, amount=self.allocation.amount)
        refund = Refund(charge=charge1, amount=charge1.amount//2)
        charge2 = Charge(allocation=charge1.allocation, amount=charge1.amount-refund.amount)
        clusterbank.model.Session.flush() # SA <= 0.4.3 has transient query bug
        try:
            clusterbank.model.Session.commit()
        except exceptions.InsufficientFunds:
            assert False, "Didn't correctly refund charge."
    
    def test_greater_amount_with_partially_refunded_charge (self):
        charge1 = Charge(allocation=self.allocation, amount=self.allocation.amount)
        refund = Refund(charge=charge1, amount=charge1.amount//2)
        charge2 = Charge(allocation=charge1.allocation, amount=(charge1.amount-refund.amount)+1)
        clusterbank.model.Session.flush() # SA <= 0.4.3 has transient query bug
        clusterbank.model.Session.commit()
        assert self.allocation.amount_available == self.allocation.amount - (charge1.amount + charge2.amount - refund.amount)
    
    def test_effective_amount (self):
        charge = Charge(allocation=self.allocation, amount=100)
        clusterbank.model.Session.flush() # SA <= 0.4.3 has transient query bug
        assert charge.effective_amount == 100
        refund1 = Refund(charge=charge, amount=10)
        assert charge.effective_amount == 90
        refund2 = Refund(charge=charge, amount=20)
        assert charge.effective_amount == 70


class TestRefund (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.allocation = Allocation(amount=1200,
            project=self.project, resource=self.resource,
            start=self.now, expiration=self.now+timedelta(days=1))
        self.charge = Charge(allocation=self.allocation, amount=900)
        clusterbank.model.Session.flush() # SA <= 0.4.3 has transient query bug
    
    def test_init (self):
        refund = Refund(id=1, datetime=self.now,
            charge=self.charge, amount=300, comment="testing")
        assert refund.id == 1
        assert refund.datetime == self.now
        assert refund.charge is self.charge
        assert refund.amount == 300
        assert refund.comment == "testing"
    
    def test_automatic_id (self):
        refund = Refund(charge=self.charge, amount=300)
        clusterbank.model.Session.commit()
        assert refund.id is not None
    
    def test_automatic_datetime (self):
        refund = Refund(charge=self.charge, amount=300)
        clusterbank.model.Session.commit()
        assert datetime.now() - refund.datetime < timedelta(minutes=1)
    
    @raises(ValueError)
    def test_negative_amount (self):
        refund = Refund()
        refund.amount = -100
        clusterbank.model.Session.commit()
    
    @raises(ValueError)
    def test_excessive_amount (self):
        refund = Refund(charge=self.charge, amount=self.charge.amount+1)
        clusterbank.model.Session.commit()
