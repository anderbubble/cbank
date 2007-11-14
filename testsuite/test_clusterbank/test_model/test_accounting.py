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


class TestRequest (AccountingTester):
    
    def test_init (self):
        request = Request(
            project=self.project, resource=self.resource,
            amount = 2000, comment = "testing",
            start = datetime(year=2007, month=1, day=1))
        assert request.project is self.project
        assert request.resource is self.resource
        assert request.amount == 2000
        assert request.start == datetime(year=2007, month=1, day=1)
        assert request.comment == "testing"
        assert not request.allocations
        clusterbank.model.Session.commit()
        assert request.id is not None
        assert request.datetime - datetime.now() < timedelta(minutes=1)
    
    def test_negative_amount (self):
        request = Request()
        try:
            request.amount = -100
        except ValueError:
            pass
        else:
            assert not "allowed negative amount"


class TestAllocation (AccountingTester):
    
    def test_init (self):
        allocation = Allocation(
            project=self.project, resource=self.resource,
            amount=1500,
            start=datetime(year=2007, month=1, day=1),
            expiration=datetime(year=2008, month=1, day=1),
            comment="testing")
        assert allocation.project is self.project
        assert allocation.resource is self.resource
        assert allocation.amount == 1500
        assert allocation.start == datetime(year=2007, month=1, day=1)
        assert allocation.expiration == datetime(year=2008, month=1, day=1)
        assert allocation.comment == "testing"
        assert not allocation.holds
        assert not allocation.charges
        clusterbank.model.Session.commit()
        assert allocation.id is not None
        assert allocation.datetime - datetime.now() < timedelta(minutes=1)
    
    
    def test_active (self):
        now = datetime.now()
        allocation = Allocation(amount=1200,
            start=now+timedelta(days=1), expiration=now+timedelta(days=2))
        assert not allocation.active
        allocation.start = now - timedelta(days=2)
        allocation.expiration = now - timedelta(days=1)
        assert not allocation.active
        allocation.expiration = now + timedelta(days=1)
        assert allocation.active


class TestCreditLimit (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
    
    def test_init (self):
        credit_limit = CreditLimit(
            project=self.project, resource=self.resource,
            start=datetime(year=2007, month=1, day=1),
            amount=100, comment="testing")
        assert credit_limit.project is self.project
        assert credit_limit.resource is self.resource
        assert credit_limit.start == datetime(year=2007, month=1, day=1)
        assert credit_limit.amount == 100
        assert credit_limit.comment == "testing"
        clusterbank.model.Session.commit()
        assert credit_limit.id is not None
        assert credit_limit.datetime - datetime.now() < timedelta(minutes=1)
    
    def test_negative (self):
        credit_limit = CreditLimit()
        try:
            credit_limit.amount = -100
        except ValueError:
            pass
        else:
            assert not "allowed a negative amount"


class TestHold (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.allocation = Allocation(amount=1200,
            project=self.project, resource=self.resource,
            start=datetime.now(), expiration=datetime.now()+timedelta(days=1))
    
    def test_init (self):
        hold = Hold(allocation=self.allocation, amount=600, comment="testing")
        assert hold.allocation is self.allocation
        assert hold.amount == 600
        assert hold.comment == "testing"
        clusterbank.model.Session.commit()
        assert hold.id is not None
        assert hold.datetime - datetime.now() < timedelta(minutes=1)
    
    def test_negative_amount (self):
        hold = Hold()
        try:
            hold.amount = -100
        except ValueError:
            pass
        else:
            assert not "allowed negative amount"
    
    def test_distributed (self):
        allocation1 = self.allocation
        allocation1.amount = 600
        allocation2 = Allocation(amount=600,
            project=self.project, resource=self.resource,
            start=datetime.now(), expiration=datetime.now()+timedelta(days=1))
        holds = Hold.distributed((allocation1, allocation2), amount=900)
        assert len(holds) == 2
        assert sum(hold.amount for hold in holds) == 900
        for hold in holds:
            assert hold.allocation in (allocation1, allocation2)


class TestCharge (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.allocation = Allocation(amount=1200,
            project=self.project, resource=self.resource,
            start=datetime.now(), expiration=datetime.now()+timedelta(days=1))
    
    def test_init_standard (self):
        charge = Charge(allocation=self.allocation, amount=900, comment="testing")
        assert charge.allocation is self.allocation
        assert charge.amount == 900
        assert charge.comment == "testing"
        clusterbank.model.Session.flush()
        assert charge.id is not None
        assert datetime.now() - charge.datetime < timedelta(minutes=1)
    
    def test_init_hold (self):
        hold = Hold(allocation=self.allocation, amount=900, comment="testing")
        charge = Charge(hold=hold)
        assert not hold.active
        assert charge.allocation is hold.allocation
        assert charge.amount == hold.amount
    
    def test_distributed (self):
        allocation1 = self.allocation
        allocation1.amount = 600
        allocation2 = Allocation(amount=600,
            project=self.project, resource=self.resource,
            start=datetime.now(), expiration=datetime.now()+timedelta(days=1))
        charges = Charge.distributed((allocation1, allocation2), amount=900)
        assert len(charges) == 2
        assert sum(charge.amount for charge in charges) == 900
        for charge in charges:
            assert charge.allocation in (allocation1, allocation2)
    
    def test_negative_amount (self):
        charge = Charge(allocation=self.allocation)
        try:
            charge.amount = -300
        except ValueError:
            pass
        else:
            assert not "allowed negative amount"


class TestRefund (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.allocation = Allocation(project=self.project, resource=self.resource,
            start=datetime.now(), expiration=datetime.now()+timedelta(days=1),
            amount=1200)
        self.charge = Charge(allocation=self.allocation, amount=900)
    
    def test_init (self):
        refund = Refund(charge=self.charge, amount=300, comment="testing")
        assert refund.charge is self.charge
        assert refund.amount == 300
        assert refund.comment == "testing"
        clusterbank.model.Session.commit()
        assert refund.id is not None
        assert datetime.now() - refund.datetime < timedelta(minutes=1)
    
    def test_negative_amount (self):
        refund = Refund()
        try:
            refund.amount = -100
        except ValueError:
            pass
        else:
            assert not "allowed negative amount"
    
    def test_excessive_amount (self):
        refund = Refund(charge=self.charge)
        try:
            refund.amount = self.charge.amount + 1
        except ValueError:
            pass
        else:
            assert not "allowed refund greater than charge"
