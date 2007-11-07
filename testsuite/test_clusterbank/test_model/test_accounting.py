from datetime import datetime, timedelta

import clusterbank.model
from clusterbank.model.entities import Project, Resource
from clusterbank.model.accounting import \
    Request, Allocation, CreditLimit, Lien, Charge, Refund

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


class TestLien (AccountingTester):
    
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
        
        liens = Lien.distributed(
            allocations = [allocation1, allocation2],
            amount = 900,
        )
        assert len(liens) == 2
        assert sum([lien.amount for lien in liens]) == 900
    
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
        
        liens = Lien.distributed(
            allocations = [allocation1, allocation2],
            amount = 900,
        )
        assert len(liens) == 2
        assert liens[0].amount == 300
        assert liens[1].amount == 600
    
    def test_distributed_to_none_negative (self):
        self.allocation.amount = 300
        credit_limit = CreditLimit(
            amount = 300,
            start = datetime.now() - timedelta(days=1),
            project = self.project,
            resource = self.resource,
        )
        lien1 = Lien(
            allocation = self.allocation,
            amount = 300,
        )
        liens = Lien.distributed(
            allocations = [self.allocation],
            amount = 300,
        )
        assert len(liens) == 1
        assert liens[0].amount == 300
    
    def test_convenience_properties (self):
        lien = Lien(
            allocation = self.allocation,
            amount = 900,
        )
        assert lien.project is self.request.project
        assert lien.resource is self.request.resource
    
    def test_effective_charge (self):
        lien = Lien(
            allocation = self.allocation,
            amount = 900,
        )
        assert lien.effective_charge == 0
        charge = Charge(lien=lien, amount=300)
        assert lien.effective_charge == 300
        refund = Refund(charge=charge, amount=100)
        assert lien.effective_charge == 300 - 100
    
    def test_amount_available (self):
        lien = Lien(
            allocation = self.allocation,
            amount = 900,
        )
        assert lien.amount_available == 900
        charge = Charge(lien=lien, amount=300)
        assert lien.amount_available == 900 - 300
        refund = Refund(charge=charge, amount=100)
        assert lien.amount_available == 900 - (300 - 100)
    
    def test_lien_not_negative (self):
        try:
            lien = Lien(
                allocation = self.allocation,
                amount = -900,
            )
        except ValueError:
            pass
        else:
            assert not "Allowed negative lien."
    
    def test_active (self):
        lien = Lien(
            allocation = self.allocation,
            amount = 900,
        )
        assert lien.active
        lien.allocation.start = datetime.now() + (timedelta(days=1) / 2)
        assert not lien.active
        lien.allocation.start = datetime.now() - timedelta(days=2)
        lien.allocation.expiration = datetime.now() - timedelta(days=1)
        assert not lien.active
    
    def test_open (self):
        lien = Lien(
            allocation = self.allocation,
            amount = 900,
        )
        assert lien.open
        charge = Charge(lien=lien, amount=300)
        assert not lien.open


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
        self.lien = Lien(
            allocation = self.allocation,
            amount = 900,
        )
    
    def test_distributed (self):
        lien1 = self.lien
        lien1.amount = 450
        lien2 = Lien(
            allocation = self.allocation,
            amount = 450,
        )
        
        charges = Charge.distributed(liens=[lien1, lien2], amount=600)
        assert len(charges) == 2
        assert sum([charge.amount for charge in charges]) == 600
    
    def test_distributed_to_negative (self):
        charges = Charge.distributed(liens=[self.lien], amount=1000)
        assert len(charges) == 1
        assert sum([charge.amount for charge in charges]) == 1000
    
    def test_distributed_to_none_negative (self):
        charge1 = Charge(lien=self.lien, amount=self.lien.amount)
        charges = Charge.distributed(liens=[self.lien], amount=100)
        assert len(charges) == 1
        assert sum([charge.amount for charge in charges]) == 100
    
    def test_negative_amount (self):
        try:
            charge = Charge(
                lien = self.lien,
                amount = -300,
            )
        except ValueError:
            pass
        else:
            assert not "allowed negative amount"
    
    def test_effective_charge (self):
        charge = Charge(
            lien = self.lien,
            amount = 300,
        )
        assert charge.effective_charge == 300
        refund = Refund(charge=charge, amount=100)
        assert charge.effective_charge == 300 - 100
    
    def test_convenience_properties (self):
        charge = Charge(
            lien = self.lien,
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
        self.lien = Lien(
            allocation = self.allocation,
            amount = 900,
        )
        self.charge = Charge(
            lien = self.lien,
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
