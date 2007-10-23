from datetime import datetime, timedelta

import clusterbank.model
from clusterbank.model.entities import User, Project, Resource
from clusterbank.model.accounting import \
    Request, Allocation, CreditLimit, Lien, Charge, Refund

class AccountingTester (object):
    
    def setup (self):
        """Create the tables before each test."""
        clusterbank.model.metadata.create_all()
        self.user = User.by_name("Python")
        self.project = Project.by_name("grail")
        self.resource = Resource.by_name("spam")
        assert not self.user.member_of(self.project)
    
    def teardown (self):
        """drop the database after each test."""
        clusterbank.model.Session.close()
        clusterbank.model.metadata.drop_all()


class TestCreditLimit (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.user.can_request = True
    
    def test_permission (self):
        try:
            credit = CreditLimit(poster=self.user, project=self.project, time=100)
        except self.user.NotPermitted:
            pass
        else:
            assert not "didn't use permissions correctly"
        self.user.can_allocate = True
        try:
            credit = CreditLimit(poster=self.user, project=self.project, time=100)
        except self.user.NotPermitted:
            assert not "didn't use permissions cprrectly"
    
    def test_negative (self):
        self.user.can_allocate = True
        try:
            credit = CreditLimit(
                poster = self.user,
                project = self.project,
                resource = self.resource,
                time = -100,
            )
        except ValueError:
            pass
        else:
            assert not "allowed a negative credit limit"


class TestRequest (AccountingTester):
    
    def test_permission_notmember_can_request (self):
        request = Request(
            poster = None,
            project = None,
            resource = self.resource,
            time = 2000,
        )
        
        self.user.can_request = True
        
        request.project = self.project
        try:
            request.poster = self.user
        except self.user.NotPermitted:
            pass
        else:
            assert not "allowed without membership"
        
        request.project = None
        request.poster = self.user
        try:
            request.project = self.project
        except self.user.NotPermitted:
            pass
        else:
            assert not "allowed without membership"
        
        request.poster.can_allocate = True
        try:
            request.project = self.project
        except self.user.NotPermitted:
            assert not "permission denied with can_allocate"
    
    def test_permission_member (self):
        user = self.project.users[0]
        try:
            request = Request(
                poster = user,
                project = self.project,
                resource = self.resource,
                time = 2000,
            )
        except user.NotPermitted:
            pass
        else:
            assert not "Allowed without can_request."
        user.can_request = True
        try:
            request = Request(
                poster = user,
                project = self.project,
                resource = self.resource,
                time = 2000,
            )
        except user.NotPermitted:
            assert not "Permission denied with can_request."
    
    def test_open (self):
        self.user.can_request = True
        self.user.can_allocate = True
        request = Request(
            poster = self.user,
            project = self.project,
            resource = self.resource,
            time = 2000,
        )
        assert request.open
        allocation = Allocation(
            poster = self.user,
            request = request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        assert not request.open


class TestAllocation (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.project.users[0].can_request = True
        self.request = Request(
            poster = self.project.users[0],
            project = self.project,
            resource = self.resource,
            time = 2000,
        )
        self.user.can_allocate = False
    
    def test_permissions (self):
        try:
            allocation = Allocation(
                request = self.request,
                poster = self.user,
                time = 1200,
                start = datetime.now(),
                expiration = datetime.now() + timedelta(days=1),
            )
        except self.user.NotPermitted:
            pass
        else:
            assert "Didn't require can_allocate."
        self.user.can_allocate = True
        try:
            allocation = Allocation(
                request = self.request,
                poster = self.user,
                time = 1200,
                start = datetime.now(),
                expiration = datetime.now() + timedelta(days=1),
            )
        except self.user.NotPermitted:
            pass
        else:
            assert "Didn't require can_allocate."
    
    def test_active (self):
        self.user.can_allocate = True
        allocation = Allocation(
            poster = self.user,
            request = self.request,
            time = 1200,
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
        self.project.users[0].can_request = True
        self.request = Request(
            poster = self.project.users[0],
            project = self.project,
            resource = self.resource,
            time = 2000,
        )
        self.user.can_allocate = True
        self.allocation = Allocation(
            request = self.request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = 1200,
        )
    
    def test_distributed (self):
        allocation1 = self.allocation
        allocation1.time = 600
        allocation2 = Allocation(
            poster = self.user,
            request = self.request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = 600,
        )
        
        self.project.users[0].can_lien = True
        liens = Lien.distributed(
            poster = self.project.users[0],
            allocations = [allocation1, allocation2],
            time = 900,
        )
        assert len(liens) == 2
        assert sum([lien.time for lien in liens]) == 900
    
    def test_distributed_to_negative (self):
        allocation1 = self.allocation
        allocation1.time = 300
        allocation2 = Allocation(
            poster = self.user,
            request = self.request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = 300,
        )
        credit_limit = CreditLimit(
            poster = self.user,
            time = 300,
            start = datetime.now(),
            project = self.project,
            resource = self.resource,
        )
        
        self.project.users[0].can_lien = True
        liens = Lien.distributed(
            poster = self.project.users[0],
            allocations = [allocation1, allocation2],
            time = 900,
        )
        assert len(liens) == 2
        assert liens[0].time == 300
        assert liens[1].time == 600
    
    def test_distributed_to_none_negative (self):
        self.allocation.time = 300
        credit_limit = CreditLimit(
            poster = self.user,
            time = 300,
            start = datetime.now() - timedelta(days=1),
            project = self.project,
            resource = self.resource,
        )
        self.project.users[0].can_lien = True
        lien1 = Lien(
            poster = self.project.users[0],
            allocation = self.allocation,
            time = 300,
        )
        liens = Lien.distributed(
            poster = self.project.users[0],
            allocations = [self.allocation],
            time = 300,
        )
        assert len(liens) == 1
        assert liens[0].time == 300
    
    def test_permissions_notmember (self):
        try:
            lien = Lien(
                allocation = self.allocation,
                poster = self.user,
                time = 900,
            )
        except self.user.NotPermitted:
            pass
        else:
            assert not "Didn't require project membership or can_lien and can_charge."
        self.user.can_lien = True
        try:
            lien = Lien(
                allocation = self.allocation,
                poster = self.user,
                time = 900,
            )
        except self.user.NotPermitted:
            pass
        else:
            assert not "Didn't require project membership or can_charge."
        self.user.can_charge = True
        try:
            lien = Lien(
                allocation = self.allocation,
                poster = self.user,
                time = 900,
            )
        except self.user.NotPermitted:
            assert not "Denied permission with can_lien and can_charge."
    
    def test_permissions_member (self):
        user = self.project.users[0]
        try:
            lien = Lien(
                allocation = self.allocation,
                poster = user,
                time = 900,
            )
        except user.NotPermitted:
            pass
        else:
            assert not "Didn't require can_lien."
        user.can_lien = True
        try:
            lien = Lien(
                allocation = self.allocation,
                poster = user,
                time = 900,
            )
        except user.NotPermitted:
            assert not "Didn't permit lien with membership and can_lien."
    
    def test_convenience_properties (self):
        self.project.users[0].can_lien = True
        lien = Lien(
            poster = self.project.users[0],
            allocation = self.allocation,
            time = 900,
        )
        assert lien.project is self.request.project
        assert lien.resource is self.request.resource
    
    def test_effective_charge (self):
        self.user.can_lien = True
        self.user.can_charge = True
        self.user.can_refund = True
        lien = Lien(
            poster = self.user,
            allocation = self.allocation,
            time = 900,
        )
        assert lien.effective_charge == 0
        charge = Charge(poster=self.user, lien=lien, time=300)
        assert lien.effective_charge == 300
        refund = Refund(poster=self.user, charge=charge, time=100)
        assert lien.effective_charge == 300 - 100
    
    def test_time_available (self):
        self.user.can_lien = True
        self.user.can_charge = True
        self.user.can_refund = True
        self.project.users[0].can_lien = True
        lien = Lien(
            poster = self.user,
            allocation = self.allocation,
            time = 900,
        )
        assert lien.time_available == 900
        charge = Charge(poster=self.user, lien=lien, time=300)
        assert lien.time_available == 900 - 300
        refund = Refund(poster=self.user, charge=charge, time=100)
        assert lien.time_available == 900 - (300 - 100)
    
    def test_lien_not_negative (self):
        self.project.users[0].can_lien = True
        try:
            lien = Lien(
                poster = self.project.users[0],
                allocation = self.allocation,
                time = -900,
            )
        except ValueError:
            pass
        else:
            assert not "Allowed negative lien."
    
    def test_active (self):
        self.project.users[0].can_lien = True
        lien = Lien(
            poster = self.project.users[0],
            allocation = self.allocation,
            time = 900,
        )
        assert lien.active
        lien.allocation.start = datetime.now() + (timedelta(days=1) / 2)
        assert not lien.active
        lien.allocation.start = datetime.now() - timedelta(days=2)
        lien.allocation.expiration = datetime.now() - timedelta(days=1)
        assert not lien.active
    
    def test_open (self):
        self.user.can_lien = True
        self.user.can_charge = True
        lien = Lien(
            poster = self.user,
            allocation = self.allocation,
            time = 900,
        )
        assert lien.open
        charge = Charge(poster=self.user, lien=lien, time=300)
        assert not lien.open


class TestCharge (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        user = self.project.users[0]
        user.can_request = True
        self.request = Request(
            poster = user,
            project = self.project,
            resource = self.resource,
            time = 2000,
        )
        self.user.can_allocate = True
        self.allocation = Allocation(
            request = self.request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = 1200,
        )
        self.project.users[0].can_lien = True
        self.lien = Lien(
            poster = self.project.users[0],
            allocation = self.allocation,
            time = 900,
        )
    
    def test_distributed (self):
        lien1 = self.lien
        lien1.time = 450
        lien2 = Lien(
            allocation = self.allocation,
            poster = self.project.users[0],
            time = 450,
        )
        
        self.user.can_charge = True
        charges = Charge.distributed(poster=self.user, liens=[lien1, lien2], time=600)
        assert len(charges) == 2
        assert sum([charge.time for charge in charges]) == 600
    
    def test_distributed_to_negative (self):
        self.user.can_charge = True
        charges = Charge.distributed(poster=self.user, liens=[self.lien], time=1000)
        assert len(charges) == 1
        assert sum([charge.time for charge in charges]) == 1000
    
    def test_distributed_to_none_negative (self):
        self.user.can_charge = True
        charge1 = Charge(poster=self.user, lien=self.lien, time=self.lien.time)
        charges = Charge.distributed(poster=self.user, liens=[self.lien], time=100)
        assert len(charges) == 1
        assert sum([charge.time for charge in charges]) == 100
    
    def test_permissions (self):
        try:
            charge = Charge(
                lien = self.lien,
                poster = self.user,
                time = 300,
            )
        except self.user.NotPermitted:
            pass
        else:
            assert not "Didn't require can_charge."
        self.user.can_charge = True
        try:
            charge = Charge(
                lien = self.lien,
                poster = self.user,
                time = 300,
            )
        except self.user.NotPermitted:
            assert not "didn't allow charge"
    
    def test_negative_time (self):
        self.user.can_charge = True
        try:
            charge = Charge(
                lien = self.lien,
                poster = self.user,
                time = -300,
            )
        except ValueError:
            pass
        else:
            assert not "allowed negative time"
    
    def test_effective_charge (self):
        self.user.can_charge = True
        self.user.can_refund = True
        charge = Charge(
            lien = self.lien,
            poster = self.user,
            time = 300,
        )
        assert charge.effective_charge == 300
        refund = Refund(poster=self.user, charge=charge, time=100)
        assert charge.effective_charge == 300 - 100
    
    def test_convenience_properties (self):
        self.user.can_charge = True
        charge = Charge(
            lien = self.lien,
            poster = self.user,
            time = 300,
        )
        assert charge.project is self.request.project
        assert charge.resource is self.request.resource


class TestRefund (AccountingTester):
    
    def setup (self):
        AccountingTester.setup(self)
        self.project.users[0].can_request = True
        self.request = Request(
            poster = self.project.users[0],
            project = self.project,
            resource = self.resource,
            time = 2000,
        )
        self.user.can_allocate = True
        self.allocation = Allocation(
            request = self.request,
            poster = self.user,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = 1200,
        )
        self.project.users[0].can_lien = True
        self.lien = Lien(
            poster = self.project.users[0],
            allocation = self.allocation,
            time = 900,
        )
        self.user.can_charge = True
        self.charge = Charge(
            lien = self.lien,
            poster = self.user,
            time = 300,
        )
    
    def test_permissions (self):
        try:
            refund = Refund(
                poster = self.user,
                charge = self.charge,
                time = 100,
            )
        except self.user.NotPermitted:
            pass
        else:
            assert not "Didn't require can_refund."
        self.user.can_refund = True
        try:
            refund = Refund(
                poster = self.user,
                charge = self.charge,
                time = 100,
            )
        except self.user.NotPermitted:
            assert not "didn't permit refund"
    
    def test_convenience_properties (self):
        self.user.can_refund = True
        refund = Refund(
            poster = self.user,
            charge = self.charge,
        )
        assert refund.project is self.request.project
        assert refund.resource is self.request.resource
    
    def test_negative_time (self):
        self.user.can_refund = True
        try:
            refund = Refund(
                poster = self.user,
                charge = self.charge,
                time = -100,
            )
        except ValueError:
            pass
        else:
            assert "Allowed negative time."
