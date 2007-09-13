from datetime import datetime, timedelta

import elixir

from clusterbank.models import \
    User, Project, Resource, \
    Request, Allocation, CreditLimit, Lien, Charge, Refund


class EntityTester (object):
    
    USER_NAME = "user1"
    PROJECT_NAME = "project3"
    RESOURCE_NAME = "resource1"
    REQUEST = 2000
    ALLOCATION = 1200
    CREDIT_LIMIT = 100
    LIEN = 900
    CHARGE = 300
    REFUND = 100
    
    def setup (self):
        """Create the tables before each test."""
        elixir.create_all()
        self.user = User.from_upstream_name(self.USER_NAME)
        self.project = Project.from_upstream_name(self.PROJECT_NAME)
        assert self.project not in self.user.projects
        self.resource = Resource.from_upstream_name(self.RESOURCE_NAME)
    
    def teardown (self):
        """drop the database after each test."""
        elixir.objectstore.clear()
        elixir.drop_all()


class UpstreamBackedEntityTester (EntityTester):
    
    def setup (self):
        EntityTester.setup(self)
        self.entity = self.Entity.from_upstream_name(self.VALID_NAME)
    
    def test_from_upstream_name (self):
        entity = self.Entity.from_upstream_name(self.VALID_NAME)
        assert isinstance(entity, self.Entity)
        
        try:
            entity = self.Entity.from_upstream_name(self.INVALID_NAME)
        except self.Entity.DoesNotExist:
            pass
        else:
            assert not "Invalid name didn't raise proper exception."
    
    def test_id (self):
        entity = self.Entity.from_upstream_name(self.VALID_NAME)
        assert entity.id == self.VALID_ID
    
    def test_name (self):
        entity = self.Entity.from_upstream_name(self.VALID_NAME)
        assert entity.name == self.VALID_NAME
    

class TestUser (UpstreamBackedEntityTester):
    
    Entity = User
    
    VALID_ID = 1
    VALID_NAME = "user1"
    VALID_PROJECTS = ["project1", "project2"]
    
    INVALID_ID = 0
    INVALID_NAME = "user0"
    INVALID_PROJECTS = ["project3", "project4"]
    
    def setup (self):
        UpstreamBackedEntityTester.setup(self)
        self.user = self.entity
        self.valid_projects = [
            Project.from_upstream_name(name)
            for name in self.VALID_PROJECTS
        ]
        self.invalid_projects = [
            Project.from_upstream_name(name)
            for name in self.INVALID_PROJECTS
        ]
        self.project = self.valid_projects[0]
        self.resource = Resource.from_upstream_name(self.RESOURCE_NAME)
    
    def test_default_permissions (self):
        assert not self.user.can_request
        assert not self.user.can_allocate
        assert not self.user.can_lien
        assert not self.user.can_charge
        assert not self.user.can_refund
    
    def test_projects (self):
        for project in self.user.projects:
            assert isinstance(project, Project)
        for project in self.valid_projects:
            assert project in self.user.projects
        for project in self.invalid_projects:
            assert not project in self.user.projects
    
    def test_member_of (self):
        for project in self.valid_projects:
            assert self.user.member_of(project)
        for project in self.invalid_projects:
            assert not self.user.member_of(project)
    
    def test_request (self):
        request = self.user.request()
        assert isinstance(request, Request)
    
    def test_allocate (self):
        request = self.user.request()
        allocation = self.user.allocate(request)
        assert isinstance(allocation, Allocation)
    
    def test_allocate_credit (self):
        credit = self.user.allocate_credit()
        assert isinstance(credit, CreditLimit)
    
    def test_lien (self):
        request = self.user.request()
        allocation = self.user.allocate(request)
        lien = self.user.lien(allocation)
        assert isinstance(lien, Lien)
    
    def test_smart_lien (self):
        self.user.can_request = True
        self.user.can_allocate = True
        
        request = self.user.request(
            project = self.project,
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation1 = self.user.allocate(request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.LIEN / 2,
        )
        allocation2 = self.user.allocate(request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.LIEN / 2,
        )
        
        elixir.objectstore.flush()
        
        liens = self.user.lien(
            project = self.project,
            resource = self.resource,
            time = self.LIEN,
        )
        assert len(liens) == 2
        assert sum([lien.time for lien in liens]) == self.LIEN
    
    def test_charge (self):
        request = self.user.request()
        allocation = self.user.allocate(request)
        lien = self.user.lien(allocation)
        charge = self.user.charge(lien)
        assert isinstance(charge, Charge)
    
    def test_charge_multiple_liens (self):
        self.user.can_request = True
        self.user.can_allocate = True
        
        request = self.user.request(
            project = self.project,
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation1 = self.user.allocate(request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.LIEN / 2,
        )
        allocation2 = self.user.allocate(request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.LIEN / 2,
        )
        
        elixir.objectstore.flush()
        
        liens = self.user.lien(
            project = self.project,
            resource = self.resource,
            time = self.LIEN,
        )
        
        charges = self.user.charge(liens=liens, time=self.LIEN)
        assert len(charges) == 2
        assert sum([charge.time for charge in charges]) == self.LIEN
    
    def test_refund (self):
        request = self.user.request()
        allocation = self.user.allocate(request)
        lien = self.user.lien(allocation)
        charge = self.user.charge(lien)
        refund = self.user.refund(charge)
        assert isinstance(refund, Refund)


class TestProject (UpstreamBackedEntityTester):
    
    Entity = Project
    
    VALID_ID = 1
    VALID_NAME = "project1"
    VALID_USERS = ['user1']
    
    INVALID_ID = 5
    INVALID_NAME = "project5"
    INVALID_USERS = ['user2']
    
    RESOURCE_NAME = "resource1"
    
    def setup (self):
        UpstreamBackedEntityTester.setup(self)
        self.project = self.entity
        self.valid_users = [
            User.from_upstream_name(name)
            for name in self.VALID_USERS
        ]
        self.invalid_users = [
            User.from_upstream_name(name)
            for name in self.INVALID_USERS
        ]
        self.user = self.valid_users[0]
        self.resource = Resource.from_upstream_name(self.RESOURCE_NAME)
    
    def test_users (self):
        for user in self.project.users:
            assert isinstance(user, User)
        for user in self.valid_users:
            assert user in self.project.users
        for user in self.invalid_users:
            assert not user in self.project.users
    
    def test_has_member (self):
        for user in self.valid_users:
            assert self.project.has_member(user)
        for user in self.invalid_users:
            assert not self.project.has_member(user)
    
    def test_resource_time_allocated (self):
        self.user.can_request = True
        self.user.can_allocate = True
        request = self.user.request(
            project = self.project,
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = self.user.allocate(request,
            time = self.ALLOCATION,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        assert self.project.resource_time_allocated(self.resource) == 0
        elixir.objectstore.flush()
        assert self.project.resource_time_allocated(self.resource) == self.ALLOCATION
    
    def test_resource_time_liened (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        request = self.user.request(
            project = self.project,
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = self.user.allocate(request,
            time = self.ALLOCATION,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        
        lien = self.user.lien(
            allocation = allocation,
            time = self.LIEN,
        )
        
        assert self.project.resource_time_liened(self.resource) == 0
        elixir.objectstore.flush()
        assert self.project.resource_time_liened(self.resource) == self.LIEN
        charge = self.user.charge(lien, time=self.CHARGE)
        elixir.objectstore.flush()
        lien.refresh()
        assert self.project.resource_time_liened(self.resource) == 0
    
    def test_resource_time_charged (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        self.user.can_refund = True
        request = self.user.request(
            project = self.project,
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = self.user.allocate(request,
            time = self.ALLOCATION,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        lien = self.user.lien(
            allocation = allocation,
            time = self.LIEN,
        )
        
        elixir.objectstore.flush()
        assert self.project.resource_time_charged(self.resource) == 0
        charge = self.user.charge(lien, time=self.CHARGE)
        elixir.objectstore.flush()
        assert self.project.resource_time_charged(self.resource) == self.CHARGE
        refund = self.user.refund(charge, time=self.REFUND)
        elixir.objectstore.flush()
        charge.refresh()
        assert self.project.resource_time_charged(self.resource) == self.CHARGE - self.REFUND
    
    def test_resource_time_available (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        self.user.can_refund = True
        request = self.user.request(
            project = self.project,
            resource = self.resource,
            time = self.REQUEST,
        )
        elixir.objectstore.flush()
        assert self.project.resource_time_available(self.resource) == 0
        
        allocation = self.user.allocate(request,
            time = self.ALLOCATION,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        elixir.objectstore.flush()
        assert self.project.resource_time_available(self.resource) == self.ALLOCATION
        
        lien = self.user.lien(
            allocation = allocation,
            time = self.LIEN,
        )
        elixir.objectstore.flush()
        assert self.project.resource_time_available(self.resource) == self.ALLOCATION - self.LIEN
        
        charge = self.user.charge(lien, time=self.CHARGE)
        elixir.objectstore.flush()
        lien.refresh()
        assert self.project.resource_time_available(request.resource) == self.ALLOCATION - self.CHARGE
        
        refund = self.user.refund(charge, time=self.REFUND)
        elixir.objectstore.flush()
        charge.refresh()
        assert self.project.resource_time_available(self.resource) == self.ALLOCATION - (self.CHARGE - self.REFUND)
    
    def test_resource_credit_limit (self):
        self.user.can_allocate = True
        assert self.project.resource_credit_limit(self.resource) == 0
        credit = self.user.allocate_credit(
            project = self.project,
            resource = self.resource,
            time = self.CREDIT_LIMIT,
            start = datetime.now() - timedelta(seconds=1),
        )
        elixir.objectstore.flush()
        self.project.refresh()
        assert self.project.resource_credit_limit(self.resource) == self.CREDIT_LIMIT


class TestResource (UpstreamBackedEntityTester):
    
    Entity = Resource
    
    VALID_ID = 1
    VALID_NAME = "resource1"
    
    INVALID_ID = 0
    INVALID_NAME = ""


class TestCreditLimit (EntityTester):
    
    Entity = CreditLimit
    
    def test_permission (self):
        credit = self.Entity(
            poster = self.user,
            project = self.project,
            time = self.CREDIT_LIMIT,
        )
        try:
            elixir.objectstore.flush()
        except self.user.NotPermitted:
            pass
        else:
            assert not "Didn't use permissions correctly."
    
    def test_negative (self):
        self.user.can_allocate = True
        credit = self.Entity(
            poster = self.user,
            project = self.project,
            resource = self.resource,
            time = -self.CREDIT_LIMIT,
        )
        try:
            elixir.objectstore.flush()
        except ValueError:
            pass
        else:
            assert not "Allowed a negative credit limit."


class TestRequest (EntityTester):
    
    def setup (self):
        EntityTester.setup(self)
        self.request = Request(
            poster = self.user,
            project = self.project,
            resource = self.resource,
            time = self.REQUEST,
        )
    
    def test_permission_notmember (self):
        assert not self.request.poster.member_of(self.request.project)
        try:
            elixir.objectstore.flush([self.request])
        except self.request.poster.NotPermitted:
            pass
        else:
            assert not "Allowed without membership and without can_request and can_allocate."
        
        self.request.poster.can_request = True
        try:
            elixir.objectstore.flush([self.request])
        except self.request.poster.NotPermitted:
            pass
        else:
            assert not "Allowed with can_request without membership."
        
        self.user.can_request = False
        self.user.can_allocate = True
        try:
            elixir.objectstore.flush([self.request])
        except self.request.poster.NotPermitted:
            pass
        else:
            assert not "Allowed without can_request."
        
        self.user.can_request = True
        try:
            elixir.objectstore.flush()
        except self.user.NotPermitted:
            assert not "Permission denied with can_request and can_allocate."
    
    def test_permission_member (self):
        self.request.project = self.request.poster.projects[0]
        try:
            elixir.objectstore.flush([self.request])
        except self.request.poster.NotPermitted:
            pass
        else:
            assert not "Allowed without can_request."
        
        self.request.poster.can_request = True
        try:
            elixir.objectstore.flush([self.request])
        except self.request.poster.NotPermitted:
            assert not "Permission denied with can_request and membership."
    
    def test_allocate (self):
        request = Request()
        allocation = request.allocate()
        assert isinstance(allocation, Allocation)
    
    def test_open (self):
        self.request.poster.can_request = True
        self.request.poster.can_allocate = True
        elixir.objectstore.flush([self.request])
        assert self.request.open
        
        self.user.can_allocate = True
        allocation = self.request.allocate(
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        elixir.objectstore.flush([allocation])
        self.request.refresh()
        assert not self.request.open


class TestAllocation (EntityTester):
    
    def test_permissions (self):
        self.user.can_request = True
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        elixir.objectstore.flush()
        
        allocation = Allocation(
            request = request,
            poster = self.user,
            time = self.ALLOCATION,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        try:
            elixir.objectstore.flush()
        except self.user.NotPermitted:
            pass
        else:
            assert "Didn't require can_allocate."
    
    def test_active (self):
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
            start = datetime.now(),
        )
        allocation = Allocation(
            poster = self.user,
            request = request,
            time = self.ALLOCATION,
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
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
            start = datetime.now(),
        )
        allocation = Allocation(
            request = request,
        )
        assert allocation.project is self.user.projects[0]
        assert allocation.resource is self.resource


class TestLien (EntityTester):
    
    def setup (self):
        EntityTester.setup(self)
        self.user.can_request = True
        self.user.can_allocate = True
        allocation = Allocation(
            request = Request(
                poster = self.user,
                project = self.project,
                resource = self.resource,
                time = self.REQUEST,
            ),
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        elixir.objectstore.flush()
        self.user.can_request = False
        self.user.can_allocate = False
        elixir.objectstore.flush([self.user])
        self.lien = Lien(
            allocation = allocation,
            poster = self.user,
            time = self.LIEN,
        )
    
    def test_permissions_notmember (self):
        assert not self.lien.poster.member_of(self.lien.project)
        try:
            elixir.objectstore.flush([self.lien])
        except self.lien.poster.NotPermitted:
            pass
        else:
            assert not "Didn't require project membership or can_lien and can_charge."
        
        self.lien.poster.can_lien = True
        try:
            elixir.objectstore.flush([self.lien])
        except self.lien.poster.NotPermitted:
            pass
        else:
            assert not "Didn't require project membership or can_charge."
        
        self.lien.poster.can_charge = True
        try:
            elixir.objectstore.flush([self.lien])
        except self.user.NotPermitted:
            assert not "Denied permission with can_lien and can_charge."
    
    def test_permissions_member (self):
        request = self.lien.allocation.request
        request.project = self.lien.poster.projects[0]
        request.poster.can_request = True
        request.poster.can_allocate = True
        elixir.objectstore.flush([request])
        
        try:
            elixir.objectstore.flush([self.lien])
        except self.lien.poster.NotPermitted:
            pass
        else:
            assert not "Didn't require can_lien."
        
        self.lien.poster.can_lien = True
        try:
            elixir.objectstore.flush([self.lien])
        except self.lien.poster.NotPermitted:
            assert not "Didn't permit lien with membership and can_lien."
    
    def test_convenience_properties (self):
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
        )
        assert lien.project is self.user.projects[0]
        assert lien.resource is self.resource
    
    def test_effective_charge (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        self.user.can_refund = True
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
        )
        elixir.objectstore.flush()
        
        assert lien.effective_charge == 0
        
        charge = self.user.charge(lien, time=self.CHARGE)
        elixir.objectstore.flush()
        lien.refresh()
        assert lien.effective_charge == self.CHARGE
        
        refund = self.user.refund(charge, time=self.REFUND)
        elixir.objectstore.flush()
        charge.refresh()
        assert lien.effective_charge == self.CHARGE - self.REFUND
    
    def test_time_available (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        self.user.can_refund = True
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
        )
        assert lien.time_available == self.LIEN
        
        charge = self.user.charge(lien, time=self.CHARGE)
        elixir.objectstore.flush()
        lien.refresh()
        assert lien.time_available == self.LIEN - self.CHARGE
        
        refund = self.user.refund(charge, time=self.REFUND)
        elixir.objectstore.flush()
        charge.refresh()
        assert lien.time_available == self.LIEN - (self.CHARGE - self.REFUND)
    
    def test_lien_not_negative (self):
        self.lien.poster.can_lien = True
        self.lien.poster.can_charge = True
        self.lien.time = -self.LIEN
        try:
            elixir.objectstore.flush([self.lien])
        except ValueError:
            pass
        else:
            assert not "Allowed negative lien."
    
    def test_charge (self):
        lien = Lien()
        charge = lien.charge()
        assert isinstance(charge, Charge)
    
    def test_active (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now() + timedelta(days=1),
            expiration = datetime.now() + timedelta(days=2),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
            datetime = datetime.now() - timedelta(days=2),
        )
        
        assert not lien.active
        
        lien.allocation.start = datetime.now() - timedelta(days=2)
        lien.allocation.expiration = datetime.now() - timedelta(days=1)
        assert not lien.active
        
        lien.allocation.expiration = datetime.now() + timedelta(days=1)
        assert lien.active
    
    def test_open (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now() - timedelta(days=1),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
            datetime = datetime.now(),
        )
        elixir.objectstore.flush()
        
        assert lien.open
        
        charge = self.user.charge(lien, time=self.CHARGE)
        elixir.objectstore.flush()
        lien.refresh()
        assert not lien.open


class TestCharge (EntityTester):
    
    def test_permissions (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
        )
        elixir.objectstore.flush()
        
        charge = Charge(
            lien = lien,
            poster = self.user,
            time = self.CHARGE,
        )
        try:
            elixir.objectstore.flush()
        except self.user.NotPermitted:
            pass
        else:
            assert not "Didn't require can_charge."
    
    def test_negative_time (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
        )
        charge = Charge(
            lien = lien,
            poster = self.user,
            time = -self.CHARGE,
        )
        
        try:
            elixir.objectstore.flush()
        except ValueError:
            pass
        else:
            assert False
    
    def test_effective_charge (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        self.user.can_refund = True
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
        )
        charge = Charge(
            lien = lien,
            poster = self.user,
            time = self.CHARGE,
        )
        elixir.objectstore.flush()
        
        assert charge.effective_charge == self.CHARGE
        
        refund = self.user.refund(charge, time=self.REFUND)
        elixir.objectstore.flush()
        charge.refresh()
        assert charge.effective_charge == self.CHARGE - self.REFUND
    
    def test_convenience_properties (self):
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
        )
        charge = Charge(
            lien = lien,
            poster = self.user,
            time = self.CHARGE,
        )
        assert charge.project is self.user.projects[0]
        assert charge.resource is self.resource
    
    def test_refund (self):
        charge = Charge()
        refund = charge.refund()
        assert isinstance(refund, Refund)


class TestRefund (EntityTester):
    
    def test_permissions (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
        )
        charge = Charge(
            lien = lien,
            poster = self.user,
            time = self.CHARGE,
        )
        elixir.objectstore.flush()
        
        refund = Refund(
            poster = self.user,
            charge = charge,
            time = self.REFUND,
        )
        try:
            elixir.objectstore.flush()
        except self.user.NotPermitted:
            pass
        else:
            assert not "Didn't require can_refund."
    
    def test_convenience_properties (self):
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
        )
        charge = Charge(
            lien = lien,
            poster = self.user,
            time = self.CHARGE,
        )
        refund = Refund(
            charge = charge,
        )
        assert refund.project is self.user.projects[0]
        assert refund.resource is self.resource
    
    def test_negative_time (self):
        self.user.can_request = True
        self.user.can_allocate = True
        self.user.can_lien = True
        self.user.can_charge = True
        self.user.can_refund = True
        request = Request(
            poster = self.user,
            project = self.user.projects[0],
            resource = self.resource,
            time = self.REQUEST,
        )
        allocation = Allocation(
            request = request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = self.ALLOCATION,
        )
        lien = Lien(
            poster = self.user,
            allocation = allocation,
            time = self.LIEN,
        )
        charge = Charge(
            lien = lien,
            poster = self.user,
            time = self.CHARGE,
        )
        refund = Refund(
            poster = charge.poster,
            charge = charge,
            time = -self.REFUND,
        )
        try:
            elixir.objectstore.flush()
        except ValueError:
            pass
        else:
            assert "Allowed negative time."


# class TestUnitFactor (EntityTester):
#     
#     UNITS = 100
#     FACTOR = 0.3
#     
#     def test_resource_factor (self):
#         factor = UnitFactor(
#             poster = self.user,
#             resource = self.resource,
#             factor = self.FACTOR,
#         )
#         elixir.objectstore.flush()
#         jitter = UnitFactor.resource_factor(self.resource) - factor.factor
#         assert jitter < 0.1
#     
#     def test_to_ru (self):
#         factor = UnitFactor(
#             poster = self.user,
#             resource = self.resource,
#             factor = self.FACTOR,
#         )
#         elixir.objectstore.flush()
#         resource_units = UnitFactor.to_ru(factor.resource, self.UNITS)
#         assert resource_units == int(self.UNITS * self.FACTOR)
#     
#     def test_to_su (self):
#         factor = UnitFactor(
#             poster = self.user,
#             resource = self.resource,
#             factor = self.FACTOR,
#         )
#         elixir.objectstore.flush()
#         standard_units = UnitFactor.to_su(factor.resource, self.UNITS)
#         print standard_units
#         assert standard_units == self.UNITS // self.FACTOR
