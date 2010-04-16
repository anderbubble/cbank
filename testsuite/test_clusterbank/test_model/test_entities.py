from nose.tools import raises, assert_equal

from datetime import datetime, timedelta

from clusterbank import config
from clusterbank.controllers import Session
from clusterbank.model import metadata, User, Project, Resource, \
    Allocation, Hold, Job, Charge, Refund

__all__ = [
    "TestUser", "TestProject", "TestResource",
    "TestAllocation", "TestHold", "TestCharge", "TestRefund",
]


def assert_in (item, container):
    assert item in container, "%s not in %s" % (item, container)


def assert_identical (item1, item2):
    assert item1 is item2, "%r is not %r" % (item1, item2)


class EntityTester (object):
    
    def setup (self):
        metadata.create_all()
    
    def teardown (self):
        Session.close()
        metadata.drop_all()


class TestUser (EntityTester):
    
    def test_id (self):
        user = User(1)
        assert user.id == 1
    
    def test_name (self):
        user = User(1)
        assert user.name == "monty"


class TestProject (EntityTester):
    
    def test_id (self):
        project = Project(1)
        assert project.id == 1
    
    def test_name (self):
        project = Project(1)
        assert project.name == "grail"


class TestResource (EntityTester):
    
    def test_id (self):
        resource = Resource(1)
        assert resource.id == 1
    
    def test_str (self):
        resource = Resource(1)
        assert_equal(str(resource), "spam")


class TestAllocation (EntityTester):
    
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
        assert allocation.end == start + timedelta(days=1)
        assert allocation.holds == []
        assert allocation.charges == []
    
    def test_active (self):
        now = datetime.now()
        allocation = Allocation(None, None, 0,
            now+timedelta(days=1), now+timedelta(days=2))
        assert not allocation.active
        allocation.start = now - timedelta(days=2)
        allocation.end = now - timedelta(days=1)
        assert not allocation.active
        allocation.end = now + timedelta(days=1)
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


class TestHold (EntityTester):
    
    def test_init (self):
        allocation = Allocation(None, None, 0, None, None)
        hold = Hold(allocation, 600)
        assert hold.id is None
        assert datetime.now() - hold.datetime < timedelta(minutes=1)
        assert hold.allocation is allocation
        assert hold.amount == 600
        assert hold.comment is None
        assert hold.active
        assert hold.job is None
    
    def test_persistence (self):
        user = User(1)
        project = Project(1)
        resource = "1"
        allocation = Allocation(project, resource, 10,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        hold = Hold(allocation, 2)
        hold.job = Job("resource1.1")
        Session.add(hold)
        Session.commit()
        Session.close()
        hold = Session.query(Hold).one()
        assert_equal(hold.id, 1)
        assert_equal(hold.allocation_id, 1)
        assert_equal(hold.amount, 2)
        assert_equal(hold.job_id, "resource1.1")
    
    @raises(ValueError)
    def test_distributed_without_allocations (self):
        holds = Hold.distributed([], amount=900)
    
    def test_distributed (self):
        start = datetime.now()
        end = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, end),
            Allocation(None, None, 600, start, end)]
        holds = Hold.distributed(allocations, amount=900)
        assert len(holds) == 2
        assert holds[0].allocation is allocations[0]
        assert holds[0].amount == 600
        assert holds[1].allocation is allocations[1]
        assert holds[1].amount == 300
    
    def test_distributed_zero_amount (self):
        start = datetime.now()
        end = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, end),
            Allocation(None, None, 600, start, end)]
        holds = Hold.distributed(allocations, amount=0)
        assert len(holds) == 1, "holds: %i" % len(holds)
        hold = holds[0]
        assert hold.amount == 0, "hold: %i" % hold.amount
        assert_in(hold.allocation, allocations)


class TestJob (EntityTester):
    
    def setup (self):
        EntityTester.setup(self)
    
    def teardown (self):
        EntityTester.teardown(self)
    
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
        assert job.charges == [] # not PBS
        assert job.holds == [] # not PBS
    
    def test_str (self):
        job = Job("www.example.com.123")
        assert str(job) == "www.example.com.123"
    
    def test_persistence (self):
        # create an example job
        user1 = User(1)
        project1 = Project(1)
        resource1 = "1"
        allocation1 = Allocation(project1, resource1, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        charges = [Charge(allocation1, 0), Charge(allocation1, 0)]
        job = Job("www.example.com.123")
        job.user = user1
        job.group = "agroup"
        job.account = project1
        job.name = "myjob"
        job.queue = "aqueue"
        job.reservation_name = "areservation"
        job.reservation_id = "www.example.com.1"
        job.ctime = datetime(2000, 1, 1)
        job.qtime = datetime(2001, 1, 1)
        job.etime = datetime(2001, 1, 2)
        job.start = datetime(2001, 2, 2)
        job.exec_host = "ANL-R00-M1-512"
        job.resource_list = {'nodes':64, 'walltime':timedelta(minutes=10),
            'otherresource':"stringvalue"}
        job.session = 123
        job.alternate_id = "anotherid"
        job.end = datetime(2001, 2, 3)
        job.exit_status = 128
        job.resources_used = {'nodes':64, 'walltime':timedelta(minutes=10),
            'otherresource':"stringvalue"}
        job.accounting_id = "someaccountingid"
        job.charges = charges
        # run the job through the database
        s = Session()
        s.add(job)
        s.flush()
        charge_ids = [charge.id for charge in charges]
        s.expunge_all()
        job = s.query(Job).filter_by(id="www.example.com.123").one()
        # check the job's attributes for persistence
        assert_equal(job.id, "www.example.com.123")
        assert_identical(job.user, s.query(User).filter_by(id=1).one())
        assert_equal(job.group, "agroup")
        assert_equal(job.account, s.query(Project).filter_by(id=1).one())
        assert_equal(job.name, "myjob")
        assert_equal(job.queue, "aqueue")
        assert_equal(job.reservation_name, "areservation")
        assert_equal(job.reservation_id, "www.example.com.1")
        assert_equal(job.ctime, datetime(2000, 1, 1))
        assert_equal(job.qtime, datetime(2001, 1, 1))
        assert_equal(job.etime, datetime(2001, 1, 2))
        assert_equal(job.start, datetime(2001, 2, 2))
        assert_equal(job.exec_host, "ANL-R00-M1-512")
        assert_equal(job.resource_list, {'nodes':64,
            'walltime':timedelta(minutes=10), 'otherresource':"stringvalue"})
        assert_equal(job.session, 123)
        assert_equal(job.alternate_id, "anotherid")
        assert_equal(job.end, datetime(2001, 2, 3))
        assert_equal(job.exit_status, 128)
        assert_equal(job.resources_used, {'nodes':64,
            'walltime':timedelta(minutes=10), 'otherresource':"stringvalue"})
        assert_equal(job.accounting_id, "someaccountingid")
        assert_equal(set(job.charges),
            set(s.query(Charge).filter(Charge.id.in_(charge_ids))))
    
    def test_dict_float (self):
        # create an example job
        job = Job("www.example.com.123")
        job.resource_list = {'afloat':1.1}
        job.resources_used = {'afloat':1.2}
        # run the job through the database
        s = Session()
        s.add(job)
        s.flush()
        s.expunge_all()
        job = s.query(Job).filter_by(id="www.example.com.123").one()
        # check the job's attributes for persistence
        assert_equal(job.resource_list, {'afloat':1.1})
        assert_equal(job.resources_used, {'afloat':1.2})
    
    def test_timedelta_days (self):
        # create an example job
        job = Job("www.example.com.123")
        job.resource_list = {'walltime':timedelta(days=1)}
        job.resources_used = {'walltime':timedelta(days=1)}
        # run the job through the database
        s = Session()
        s.add(job)
        s.flush()
        s.expunge_all()
        job = s.query(Job).filter_by(id="www.example.com.123").one()
        # check the job's attributes for persistence
        assert_equal(job.resource_list, {'walltime':timedelta(days=1)})
        assert_equal(job.resources_used, {'walltime':timedelta(days=1)})
    
    def test_timedelta_microseconds (self):
        # create an example job
        job = Job("www.example.com.123")
        job.resource_list = {'walltime':timedelta(microseconds=1)}
        job.resources_used = {'walltime':timedelta(microseconds=1)}
        # run the job through the database
        s = Session()
        s.add(job)
        s.flush()
        s.expunge_all()
        job = s.query(Job).filter_by(id="www.example.com.123").one()
        # check the job's attributes for persistence
        assert_equal(job.resource_list, {'walltime':timedelta(microseconds=1)})
        assert_equal(job.resources_used, {'walltime':timedelta(microseconds=1)})


class TestCharge (EntityTester):
    
    def test_init (self):
        now = datetime.now()
        allocation = Allocation(None, None, 0, None, None)
        charge = Charge(allocation, 600)
        assert charge.id is None
        assert charge.datetime - now < timedelta(minutes=1)
        assert charge.allocation is allocation
        assert charge.amount == 600
        assert charge.comment is None
        assert charge.refunds == []
        assert charge.job is None
    
    @raises(ValueError)
    def test_distributed_without_allocations (self):
        charges = Charge.distributed([], amount=900)
    
    def test_distributed (self):
        start = datetime.now()
        end = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, end),
            Allocation(None, None, 600, start, end)]
        charges = Charge.distributed(allocations, amount=900)
        assert len(charges) == 2
        assert sum(charge.amount for charge in charges) == 900
        assert charges[0].allocation is allocations[0]
        assert charges[0].amount == 600
        assert charges[1].allocation is allocations[1]
        assert charges[1].amount == 300
    
    def test_distributed_zero_amount (self):
        start = datetime.now()
        end = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, end),
            Allocation(None, None, 600, start, end)]
        charges = Charge.distributed(allocations, amount=0)
        assert len(charges) == 1, "charges: %i" % len(charges)
        charge = charges[0]
        assert charge.amount == 0, "charge: %i" % charge.amount
        assert charge.allocation == allocations[0]
    
    def test_distributed_with_insufficient_allocation (self):
        start = datetime.now()
        end = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, end),
            Allocation(None, None, 600, start, end)]
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


class TestRefund (EntityTester):
    
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

