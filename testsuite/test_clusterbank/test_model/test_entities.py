from datetime import datetime, timedelta

import clusterbank.model
from clusterbank.model.entities import User, Project, Resource
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
        clusterbank.model.Session.clear()
        clusterbank.model.metadata.drop_all()
    

class TestUser (EntityTester):
    
    def setup (self):
        EntityTester.setup(self)
        self.user = User.by_name("Monty")
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        self.user.can_refund = True
    
    def test_name (self):
        assert self.user.name == "Monty"
    
    def test_default_permissions (self):
        user = User.by_name("Python")
        assert not user.can_request
        assert not user.can_allocate
        assert not user.can_lien
        assert not user.can_charge
        assert not user.can_refund
    
    def test_projects (self):
        for project in self.user.projects:
            assert isinstance(project, Project)
        for project_name in ["grail", "meaning"]:
            assert Project.by_name(project_name) in self.user.projects
        for project_name in ["brian", "circus"]:
            assert not Project.by_name(project_name) in self.user.projects
    
    def test_member_of (self):
        for project_name in ["grail", "meaning"]:
            assert self.user.member_of(Project.by_name(project_name))
        for project_name in ["brian", "circus"]:
            assert not self.user.member_of(Project.by_name(project_name))
    
    def test_request (self):
        request = self.user.request()
        assert isinstance(request, Request)
    
    def test_allocate (self):
        request = self.user.request()
        allocation = self.user.allocate(request=request)
        assert isinstance(allocation, Allocation)
    
    def test_allocate_credit (self):
        credit = self.user.allocate_credit()
        assert isinstance(credit, CreditLimit)
    
    def test_lien (self):
        request = self.user.request()
        allocation = self.user.allocate(request=request)
        lien = self.user.lien(allocation=allocation)
        assert isinstance(lien, Lien)
    
    def test_smart_lien (self):
        request = self.user.request(
            project = self.user.projects[0],
            resource = Resource.by_name("spam"),
            time = 1024,
        )
        allocation1 = self.user.allocate(
            request = request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = 512,
        )
        allocation2 = self.user.allocate(
            request = request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = 512,
        )
        
        liens = self.user.lien(
            project = request.project,
            resource = request.resource,
            time = 768,
        )
        assert len(liens) == 2
        assert sum([lien.time for lien in liens]) == 768
    
    def test_charge (self):
        request = self.user.request()
        allocation = self.user.allocate(request=request)
        lien = self.user.lien(allocation=allocation)
        charge = self.user.charge(lien=lien)
        assert isinstance(charge, Charge)
    
    def test_charge_multiple_liens (self):
        request = self.user.request(
            project = self.user.projects[0],
            resource = Resource.by_name("spam"),
            time = 1024,
        )
        allocation1 = self.user.allocate(
            request = request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = 512,
        )
        allocation2 = self.user.allocate(
            request = request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = 512,
        )
        
        liens = self.user.lien(
            project = request.project,
            resource = request.resource,
            time = 1024,
        )
        
        charges = self.user.charge(liens=liens, time=768)
        assert len(charges) == 2
        assert sum([charge.time for charge in charges]) == 768
    
    def test_refund (self):
        request = self.user.request()
        allocation = self.user.allocate(request=request)
        lien = self.user.lien(allocation=allocation)
        charge = self.user.charge(lien=lien)
        refund = self.user.refund(charge=charge)
        assert isinstance(refund, Refund)


class TestProject (EntityTester):
    
    def setup (self):
        EntityTester.setup(self)
        self.project = Project.by_name("grail")
        self.user = self.project.users[0]
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        self.user.can_refund = True
    
    def test_name (self):
        assert self.project.name == "grail"
    
    def test_users (self):
        assert self.project.users
        for user in self.project.users:
            assert isinstance(user, User)
        assert User.by_name("Monty") in self.project.users
        assert not User.by_name("Python") in self.project.users
    
    def test_has_member (self):
        for user in self.project.users:
            assert self.project.has_member(user)
        assert not self.project.has_member(User.by_name("Python"))
    
    def test_time_allocated (self):
        spam = Resource.by_name("spam")
        request = self.user.request(
            project = self.project,
            resource = spam,
            time = 1024,
        )
        
        assert self.project.time_allocated(spam) == 0
        allocation = self.user.allocate(
            request = request,
            time = 1024,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        assert self.project.time_allocated(spam) == 1024
    
    def test_time_liened (self):
        spam = Resource.by_name("spam")
        request = self.user.request(
            project = self.project,
            resource = spam,
            time = 1024,
        )
        allocation = self.user.allocate(
            request = request,
            time = 1024,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        
        assert self.project.time_liened(spam) == 0
        lien = self.user.lien(allocation=allocation, time=512)
        assert self.project.time_liened(spam) == 512
        charge = self.user.charge(lien=lien, time=256)
        assert self.project.time_liened(spam) == 0
    
    def test_time_charged (self):
        spam = Resource.by_name("spam")
        request = self.user.request(
            project = self.project,
            resource = spam,
            time = 1024,
        )
        allocation = self.user.allocate(
            request = request,
            time = 1024,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        lien = self.user.lien(
            allocation = allocation,
            time = 512,
        )
        
        assert self.project.time_charged(spam) == 0
        charge = self.user.charge(lien=lien, time=256)
        assert self.project.time_charged(spam) == 256
        refund = self.user.refund(charge=charge, time=64)
        assert self.project.time_charged(spam) == 192
    
    def test_time_available (self):
        spam = Resource.by_name("spam")
        request = self.user.request(
            project = self.project,
            resource = spam,
            time = 1024,
        )
        
        assert self.project.time_available(spam) == 0
        allocation = self.user.allocate(
            request = request,
            time = 512,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        assert self.project.time_available(spam) == 512
        lien = self.user.lien(allocation=allocation, time=128)
        assert self.project.time_available(spam) == 384
        charge = self.user.charge(lien=lien, time=64)
        assert self.project.time_available(spam) == 448
        refund = self.user.refund(charge=charge, time=16)
        assert self.project.time_available(spam) == 464
    
    def test_credit_limit (self):
        spam = Resource.by_name("spam")
        assert self.project.credit_limit(spam) == 0
        credit = self.user.allocate_credit(
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
