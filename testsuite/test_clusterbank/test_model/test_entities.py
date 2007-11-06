from datetime import datetime, timedelta

import clusterbank.model
from clusterbank.model.entities import Project, Resource
from clusterbank.model.accounting import \
    Request, Allocation, Lien, Charge, Refund, CreditLimit

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
    
    def test_time_allocated (self):
        spam = Resource.by_name("spam")
        request = Request(
            project = self.project,
            resource = spam,
            time = 1024,
        )
        
        assert self.project.time_allocated(spam) == 0
        allocation = Allocation(
            request = request,
            time = 1024,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        assert self.project.time_allocated(spam) == 1024
    
    def test_time_liened (self):
        spam = Resource.by_name("spam")
        request = Request(
            project = self.project,
            resource = spam,
            time = 1024,
        )
        allocation = Allocation(
            request = request,
            time = 1024,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        
        assert self.project.time_liened(spam) == 0
        lien = Lien(allocation=allocation, time=512)
        assert self.project.time_liened(spam) == 512
        charge = Charge(lien=lien, time=256)
        assert self.project.time_liened(spam) == 0
    
    def test_time_charged (self):
        spam = Resource.by_name("spam")
        request = Request(
            project = self.project,
            resource = spam,
            time = 1024,
        )
        allocation = Allocation(
            request = request,
            time = 1024,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        lien = Lien(
            allocation = allocation,
            time = 512,
        )
        
        assert self.project.time_charged(spam) == 0
        charge = Charge(lien=lien, time=256)
        assert self.project.time_charged(spam) == 256
        refund = Refund(charge=charge, time=64)
        assert self.project.time_charged(spam) == 192
    
    def test_time_available (self):
        spam = Resource.by_name("spam")
        request = Request(
            project = self.project,
            resource = spam,
            time = 1024,
        )
        
        assert self.project.time_available(spam) == 0
        allocation = Allocation(
            request = request,
            time = 512,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        assert self.project.time_available(spam) == 512
        lien = Lien(allocation=allocation, time=128)
        assert self.project.time_available(spam) == 384
        charge = Charge(lien=lien, time=64)
        assert self.project.time_available(spam) == 448
        refund = Refund(charge=charge, time=16)
        assert self.project.time_available(spam) == 464
    
    def test_credit_limit (self):
        spam = Resource.by_name("spam")
        assert self.project.credit_limit(spam) == 0
        credit = CreditLimit(
            project = self.project,
            resource = spam,
            time = 128,
            start = datetime.now() - timedelta(seconds=1),
        )
        assert self.project.credit_limit(spam) == 128


class TestResource (EntityTester):
    
    def setup (self):
        EntityTester.setup(self)
        self.resource = Resource.by_name("spam")
    
    def test_name (self):
        assert self.resource.name == "spam"
