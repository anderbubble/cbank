from datetime import datetime, timedelta

import clusterbank.model
from clusterbank.model.entities import Project, Resource
from clusterbank.model.accounting import \
    Request, Allocation, CreditLimit, Hold, Charge, Refund

class AccountingTester (object):
    
    def setup (self):
        """Create the tables before each test."""
        clusterbank.model.metadata.create_all()
        self.project = Project.by_name("grail")
        self.resource = Resource.by_name("spam")
    
    def teardown (self):
        """drop the database after each test."""
        clusterbank.model.Session.close()
        clusterbank.model.metadata.drop_all()


class TestCreditLimit (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
    
    def test_negative (self):
        try:
            credit = CreditLimit(
                project = self.project,
                resource = self.resource,
                amount = -100,
            )
        except ValueError:
            pass
        else:
            assert not "allowed a negative credit limit"


class TestRequest (AccountingTester):
    
    def test_open (self):
        request = Request(
            project = self.project,
            resource = self.resource,
            amount = 2000,
        )
        assert request.open
        allocation = Allocation(
            request = request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        assert not request.open


class TestAllocation (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.request = Request(
            project = self.project,
            resource = self.resource,
            amount = 2000,
        )
    
    def test_active (self):
        allocation = Allocation(
            request = self.request,
            amount = 1200,
            start = datetime.now() + timedelta(days=1),
            expiration = datetime.now() + timedelta(days=2),
        )
        assert not allocation.active
        allocation.start = datetime.now() - timedelta(days=2)
        allocation.expiration = datetime.now() - timedelta(days=1)
        assert not allocation.active
        allocation.expiration = datetime.now() + timedelta(days=1)
        assert allocation.active
    
    def test_convenience_properties (self):
        allocation = Allocation(
            request = self.request,
        )
        assert allocation.project is self.request.project
        assert allocation.resource is self.request.resource


class TestHold (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.request = Request(
            project = self.project,
            resource = self.resource,
            amount = 2000,
        )
        self.allocation = Allocation(
            request = self.request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            amount = 1200,
        )
    
    def test_distributed (self):
        allocation1 = self.allocation
        allocation1.amount = 600
        allocation2 = Allocation(
            request = self.request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            amount = 600,
        )
        
        holds = Hold.distributed(
            allocations = [allocation1, allocation2],
            amount = 900,
        )
        assert len(holds) == 2
        assert sum([hold.amount for hold in holds]) == 900
    
    def test_distributed_to_negative (self):
        allocation1 = self.allocation
        allocation1.amount = 300
        allocation2 = Allocation(
            request = self.request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            amount = 300,
        )
        credit_limit = CreditLimit(
            amount = 300,
            start = datetime.now(),
            project = self.project,
            resource = self.resource,
        )
        
        holds = Hold.distributed(
            allocations = [allocation1, allocation2],
            amount = 900,
        )
        assert len(holds) == 2
        assert holds[0].amount == 300
        assert holds[1].amount == 600
    
    def test_distributed_to_none_negative (self):
        self.allocation.amount = 300
        credit_limit = CreditLimit(
            amount = 300,
            start = datetime.now() - timedelta(days=1),
            project = self.project,
            resource = self.resource,
        )
        hold1 = Hold(
            allocation = self.allocation,
            amount = 300,
        )
        holds = Hold.distributed(
            allocations = [self.allocation],
            amount = 300,
        )
        assert len(holds) == 1
        assert holds[0].amount == 300
    
    def test_convenience_properties (self):
        hold = Hold(
            allocation = self.allocation,
            amount = 900,
        )
        assert hold.project is self.request.project
        assert hold.resource is self.request.resource
    
    def test_effective_charge (self):
        hold = Hold(
            allocation = self.allocation,
            amount = 900,
        )
        assert hold.effective_charge == 0
        charge = Charge(hold=hold, amount=300)
        assert hold.effective_charge == 300
        refund = Refund(charge=charge, amount=100)
        assert hold.effective_charge == 300 - 100
    
    def test_amount_available (self):
        hold = Hold(
            allocation = self.allocation,
            amount = 900,
        )
        assert hold.amount_available == 900
        charge = Charge(hold=hold, amount=300)
        assert hold.amount_available == 900 - 300
        refund = Refund(charge=charge, amount=100)
        assert hold.amount_available == 900 - (300 - 100)
    
    def test_hold_not_negative (self):
        try:
            hold = Hold(
                allocation = self.allocation,
                amount = -900,
            )
        except ValueError:
            pass
        else:
            assert not "Allowed negative hold."
    
    def test_active (self):
        hold = Hold(
            allocation = self.allocation,
            amount = 900,
        )
        assert hold.active
        hold.allocation.start = datetime.now() + (timedelta(days=1) / 2)
        assert not hold.active
        hold.allocation.start = datetime.now() - timedelta(days=2)
        hold.allocation.expiration = datetime.now() - timedelta(days=1)
        assert not hold.active
    
    def test_open (self):
        hold = Hold(
            allocation = self.allocation,
            amount = 900,
        )
        assert hold.open
        charge = Charge(hold=hold, amount=300)
        assert not hold.open


class TestCharge (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.request = Request(
            project = self.project,
            resource = self.resource,
            amount = 2000,
        )
        self.allocation = Allocation(
            request = self.request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            amount = 1200,
        )
        self.hold = Hold(
            allocation = self.allocation,
            amount = 900,
        )
    
    def test_distributed (self):
        hold1 = self.hold
        hold1.amount = 450
        hold2 = Hold(
            allocation = self.allocation,
            amount = 450,
        )
        
        charges = Charge.distributed(holds=[hold1, hold2], amount=600)
        assert len(charges) == 2
        assert sum([charge.amount for charge in charges]) == 600
    
    def test_distributed_to_negative (self):
        charges = Charge.distributed(holds=[self.hold], amount=1000)
        assert len(charges) == 1
        assert sum([charge.amount for charge in charges]) == 1000
    
    def test_distributed_to_none_negative (self):
        charge1 = Charge(hold=self.hold, amount=self.hold.amount)
        charges = Charge.distributed(holds=[self.hold], amount=100)
        assert len(charges) == 1
        assert sum([charge.amount for charge in charges]) == 100
    
    def test_negative_amount (self):
        try:
            charge = Charge(
                hold = self.hold,
                amount = -300,
            )
        except ValueError:
            pass
        else:
            assert not "allowed negative amount"
    
    def test_effective_charge (self):
        charge = Charge(
            hold = self.hold,
            amount = 300,
        )
        assert charge.effective_charge == 300
        refund = Refund(charge=charge, amount=100)
        assert charge.effective_charge == 300 - 100
    
    def test_convenience_properties (self):
        charge = Charge(
            hold = self.hold,
            amount = 300,
        )
        assert charge.project is self.request.project
        assert charge.resource is self.request.resource


class TestRefund (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.request = Request(
            project = self.project,
            resource = self.resource,
            amount = 2000,
        )
        self.allocation = Allocation(
            request = self.request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            amount = 1200,
        )
        self.hold = Hold(
            allocation = self.allocation,
            amount = 900,
        )
        self.charge = Charge(
            hold = self.hold,
            amount = 300,
        )
    
    def test_convenience_properties (self):
        refund = Refund(
            charge = self.charge,
        )
        assert refund.project is self.request.project
        assert refund.resource is self.request.resource
    
    def test_negative_amount (self):
        try:
            refund = Refund(
                charge = self.charge,
                amount = -100,
            )
        except ValueError:
            pass
        else:
            assert "Allowed negative amount."
