from datetime import datetime, timedelta

import clusterbank.model
from clusterbank.model.entities import Project, Resource
from clusterbank.model.accounting import \
    Request, Allocation, Hold, Charge, Refund, CreditLimit

__all__ = [
    "TestUser", "TestProject", "TestResource",
]

class EntityTester (object):
    
    def setup (self):
        """Create the tables before each test."""
        clusterbank.model.metadata.create_all()
    
    def teardown (self):
        """drop the database after each test."""
        clusterbank.model.Session.close()
        clusterbank.model.metadata.drop_all()


class TestProject (EntityTester):
    
    def setup (self):
        EntityTester.setup(self)
        self.project = Project.by_name("grail")
    
    def test_name (self):
        assert self.project.name == "grail"
    
    def test_amount_allocated (self):
        spam = Resource.by_name("spam")
        request = Request(
            project = self.project,
            resource = spam,
            amount = 1024,
        )
        
        assert self.project.amount_allocated(spam) == 0
        allocation = Allocation(
            request = request,
            amount = 1024,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        assert self.project.amount_allocated(spam) == 1024
    
    def test_amount_held (self):
        spam = Resource.by_name("spam")
        request = Request(
            project = self.project,
            resource = spam,
            amount = 1024,
        )
        allocation = Allocation(
            request = request,
            amount = 1024,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        
        assert self.project.amount_held(spam) == 0
        hold = Hold(allocation=allocation, amount=512)
        assert self.project.amount_held(spam) == 512
        charge = Charge(hold=hold, amount=256)
        assert self.project.amount_held(spam) == 0
    
    def test_amount_charged (self):
        spam = Resource.by_name("spam")
        request = Request(
            project = self.project,
            resource = spam,
            amount = 1024,
        )
        allocation = Allocation(
            request = request,
            amount = 1024,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        hold = Hold(
            allocation = allocation,
            amount = 512,
        )
        
        assert self.project.amount_charged(spam) == 0
        charge = Charge(hold=hold, amount=256)
        assert self.project.amount_charged(spam) == 256
        refund = Refund(charge=charge, amount=64)
        assert self.project.amount_charged(spam) == 192
    
    def test_amount_available (self):
        spam = Resource.by_name("spam")
        request = Request(
            project = self.project,
            resource = spam,
            amount = 1024,
        )
        
        assert self.project.amount_available(spam) == 0
        allocation = Allocation(
            request = request,
            amount = 512,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        assert self.project.amount_available(spam) == 512
        hold = Hold(allocation=allocation, amount=128)
        assert self.project.amount_available(spam) == 384
        charge = Charge(hold=hold, amount=64)
        assert self.project.amount_available(spam) == 448
        refund = Refund(charge=charge, amount=16)
        assert self.project.amount_available(spam) == 464
    
    def test_credit_limit (self):
        spam = Resource.by_name("spam")
        assert self.project.credit_limit(spam) == 0
        credit = CreditLimit(
            project = self.project,
            resource = spam,
            amount = 128,
            start = datetime.now() - timedelta(seconds=1),
        )
        assert self.project.credit_limit(spam) == 128


class TestResource (EntityTester):
    
    def setup (self):
        EntityTester.setup(self)
        self.resource = Resource.by_name("spam")
    
    def test_name (self):
        assert self.resource.name == "spam"
