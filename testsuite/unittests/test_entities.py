from nose.tools import raises, assert_equal, assert_almost_equals

from datetime import datetime, timedelta

from sqlalchemy import create_engine

from cbank import config
from cbank.controllers import Session
from cbank.model import (
    metadata, User, Project, Resource,
    Allocation, Hold, Job, Charge, Refund)
import cbank.upstreams.default


def setup ():
    cbank.model.metadata.bind = \
        create_engine("sqlite:///:memory:")
    cbank.model.use_upstream(cbank.upstreams.default)
    cbank.upstreams.default.users = [
        cbank.upstreams.default.User("1", "monty")]
    cbank.upstreams.default.projects = [
        cbank.upstreams.default.Project("1", "grail")]
    cbank.upstreams.default.resources = [
        cbank.upstreams.default.Resource("1", "spam")]


def teardown ():
    cbank.upstreams.default.users = []
    cbank.upstreams.default.projects = []
    cbank.upstreams.default.resources = []
    cbank.model.clear_upstream()
    cbank.model.metadata.bind = None


class MappedEntityTester (object):
    
    def setup (self):
        metadata.create_all()
    
    def teardown (self):
        Session.close()
        metadata.drop_all()        


class TestUser (object):
    
    def test_cached (self):
        assert_identical(User.cached("1"), User.cached("1"))
        assert_not_identical(User.cached("1"), User.cached("2"))
        assert_identical(User.cached("2"), User.cached("2"))
    
    def test_str (self):
        assert_equal(str(User.cached("1")), "monty")
        assert_equal(str(User.cached("2")), "2")


class TestProject (object):
    
    def test_cached (self):
        assert_identical(Project.cached("1"), Project.cached("1"))
        assert_not_identical(Project.cached("1"), Project.cached("2"))
        assert_identical(Project.cached("2"), Project.cached("2"))
    
    def test_str (self):
        assert_equal(str(Project.cached("1")), "grail")
        assert_equal(str(Project.cached("2")), "2")


class TestResource (object):
    
    def test_cached (self):
        assert_identical(Resource.cached("1"), Resource.cached("1"))
        assert_not_identical(Resource.cached("1"), Resource.cached("2"))
        assert_identical(Resource.cached("2"), Resource.cached("2"))
    
    def test_str (self):
        assert_equal(str(Resource.cached("1")), "spam")
        assert_equal(str(Resource.cached("2")), "2")


class TestAllocation (MappedEntityTester):
    
    def test_init (self):
        start = datetime.now()
        end = start+timedelta(days=1)
        project = Project.cached("1")
        resource = Resource.cached("1")
        allocation = Allocation(
            project, resource, 1500, start, end)
        assert_equal(allocation.id, None)
        assert_dt_almost_equals(allocation.datetime, datetime.now())
        assert_identical(allocation.project, project)
        assert_identical(allocation.resource, resource)
        assert_equal(allocation.amount, 1500)
        assert_equal(allocation.comment, None)
        assert_equal(allocation.start, start)
        assert_equal(allocation.end, end)
        assert_equal(allocation.holds, [])
        assert_equal(allocation.charges, [])

    def test_persistence (self):
        start = datetime.now()
        end = start+timedelta(days=1)
        project = Project.cached("1")
        resource = Resource.cached("1")
        allocation = Allocation(
            project, resource, 1500, start, end)
        allocation.comment = "test"
        Session.add(allocation)
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation.id, 1)
        assert_dt_almost_equals(allocation.datetime, datetime.now())
        assert_identical(allocation.project, project)
        assert_identical(allocation.resource, resource)
        assert_equal(allocation.amount, 1500)
        assert_equal(allocation.comment, "test")
        assert_equal(allocation.start, start)
        assert_equal(allocation.end, end)
        assert_equal(allocation.holds, [])
        assert_equal(allocation.charges, [])

    def test_active (self):
        start = now = datetime.now()
        end = start+timedelta(days=2)
        allocation = Allocation(None, None, 0, start, end)
        assert allocation.active()
        assert not allocation.active(start-timedelta(hours=1))
        assert allocation.active(start)
        assert not allocation.active(end)
        assert not allocation.active(end+timedelta(hours=1))

    def test_amount_available (self):
        allocation = Allocation(None, None, 1200, None, None)
        assert_equal(allocation.amount_available(), 1200)
    
    def test_amount_available_with_active_hold (self):
        allocation = Allocation(None, None, 1200, None, None)
        hold = Hold(allocation, 900)
        assert_equal(allocation.amount_available(), 300)
    
    def test_amount_available_with_inactive_hold (self):
        allocation = Allocation(None, None, 1200, None, None)
        hold = Hold(allocation, allocation.amount)
        hold.active = False
        assert_equal(allocation.amount_available(), 1200)
    
    def test_amount_available_with_charge (self):
        allocation = Allocation(None, None, 1200, None, None)
        Charge(allocation, 600)
        assert_equal(allocation.amount_available(), 600)

    def test_amount_available_with_multiple_charges (self):
        allocation = Allocation(None, None, 1200, None, None)
        Charge(allocation, 500)
        Charge(allocation, 500)
        assert_equal(allocation.amount_available(), 200)

    def test_amount_available_with_charge_refund (self):
        allocation = Allocation(None, None, 1200, None, None)
        charge = Charge(allocation, 600)
        Refund(charge, 400)
        assert_equal(allocation.amount_available(), 1000)
    
    def test_amount_available_with_greater_charge (self):
        allocation = Allocation(None, None, 1200, None, None)
        charge = Charge(allocation, 1300)
        assert_equal(allocation.amount_available(), -100)
    

class TestHold (MappedEntityTester):
    
    def test_init (self):
        allocation = Allocation(None, None, 0, None, None)
        hold = Hold(allocation, 600)
        assert_equal(hold.id, None)
        assert_dt_almost_equals(hold.datetime, datetime.now())
        assert_identical(hold.allocation, allocation)
        assert_equal(hold.amount, 600)
        assert_equal(hold.comment, None)
        assert hold.active
        assert_equal(hold.job, None)
    
    def test_persistence (self):
        user = User.cached("1")
        project = Project.cached("1")
        resource = Resource.cached("1")
        allocation = Allocation(project, resource, 10,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        hold = Hold(allocation, 2)
        hold.job = Job("spam.1")
        Session.add(hold)
        Session.commit()
        Session.close()
        hold = Session.query(Hold).one()
        assert_equal(hold.id, 1)
        assert_equal(hold.allocation_id, 1)
        assert_equal(hold.amount, 2)
        assert_equal(hold.job_id, "spam.1")
    
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
        assert_equal(len(holds), 2)
        assert_identical(holds[0].allocation, allocations[0])
        assert_equal(holds[0].amount, 600)
        assert_identical(holds[1].allocation, allocations[1])
        assert_equal(holds[1].amount, 300)
    
    def test_distributed_zero_amount (self):
        start = datetime.now()
        end = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, end),
            Allocation(None, None, 600, start, end)]
        holds = Hold.distributed(allocations, amount=0)
        assert_equal(len(holds), 1)
        hold = holds[0]
        assert_equal(hold.amount, 0)
        assert_in(hold.allocation, allocations)


class TestJob (MappedEntityTester):
    
    def test_init (self):
        job = Job("www.example.com.123")
        assert_equal(job.id, "www.example.com.123")
        assert_equal(job.user, None)
        assert_equal(job.group, None)
        assert_equal(job.account, None)
        assert_equal(job.name, None)
        assert_equal(job.queue, None)
        assert_equal(job.reservation_name, None)
        assert_equal(job.reservation_id, None)
        assert_equal(job.ctime, None)
        assert_equal(job.qtime, None)
        assert_equal(job.etime, None)
        assert_equal(job.start, None)
        assert_equal(job.exec_host, None)
        assert_equal(job.resource_list, {})
        assert_equal(job.session, None)
        assert_equal(job.alternate_id, None)
        assert_equal(job.end, None)
        assert_equal(job.exit_status, None)
        assert_equal(job.resources_used, {})
        assert_equal(job.accounting_id, None)
        assert_equal(job.charges, []) # not PBS
        assert_equal(job.holds, []) # not PBS
    
    def test_persistence (self):
        # create an example job
        user1 = User.cached("1")
        project1 = Project.cached("1")
        resource1 = Resource.cached("1")
        allocation1 = Allocation(project1, resource1, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        charges = [Charge(allocation1, 0), Charge(allocation1, 0)]
        job = Job("www.example.com.123")
        job.user = user1
        job.group = "agroup"
        job.account = Project.fetch("project1")
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
        assert_identical(job.user, User.cached("1"))
        assert_equal(job.group, "agroup")
        assert_equal(job.account, Project.fetch("project1"))
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

    def test_from_pbs_q (self):
        entry = "04/18/2008 02:10:12;Q;692009.jmayor5.lcrc.anl.gov;queue=shared"
        job_ = Job.from_pbs(entry)
        assert job_.id == "692009.jmayor5.lcrc.anl.gov"
        assert job_.queue == "shared"
    
    def test_from_pbs_s (self):
        entry = "04/18/2008 02:10:12;S;692009.jmayor5.lcrc.anl.gov;user=monty group=agroup account=grail jobname=myjob queue=shared ctime=1208502612 qtime=1208502612 etime=1208502612 start=1208502612 exec_host=j340/0+j341/0+j342/0+j343/0+j344/0+j345/0+j346/0+j347/0 Resource_List.ncpus=8 Resource_List.neednodes=8 Resource_List.nodect=8 Resource_List.nodes=8 Resource_List.walltime=05:00:00"
        job_ = Job.from_pbs(entry)
        assert_equal(job_.id, "692009.jmayor5.lcrc.anl.gov")
        assert_identical(job_.user, User.fetch("monty"))
        assert_equal(job_.group, "agroup")
        assert_equal(job_.account, Project.fetch("grail"))
        assert_equal(job_.name, "myjob")
        assert_equal(job_.queue, "shared")
        assert_equal(job_.ctime, datetime.fromtimestamp(1208502612))
        assert_equal(job_.qtime, datetime.fromtimestamp(1208502612))
        assert_equal(job_.etime, datetime.fromtimestamp(1208502612))
        assert_equal(job_.start, datetime.fromtimestamp(1208502612))
        assert_equal(job_.exec_host,
            "j340/0+j341/0+j342/0+j343/0+j344/0+j345/0+j346/0+j347/0")
        assert_equal(job_.resource_list, {'ncpus':8, 'neednodes':8,
            'nodect':8, 'nodes':8, 'walltime':timedelta(hours=5)})
    
    def test_from_pbs_e (self):
        entry = "04/18/2008 03:35:28;E;691908.jmayor5.lcrc.anl.gov;user=monty group=agroup account=grail jobname=myjob queue=pri4 ctime=1208378066 qtime=1208378066 etime=1208378066 start=1208378066 exec_host=j75/0+j76/0+j77/0+j78/0+j79/0+j86/0+j87/0+j88/0+j89/0+j90/0+j91/0+j93/0+j94/0+j100/0+j101/0+j102/0+j103/0+j104/0+j105/0+j106/0+j107/0+j108/0+j109/0+j110/0 Resource_List.ncpus=24 Resource_List.neednodes=24 Resource_List.nodect=24 Resource_List.nodes=24 Resource_List.walltime=36:00:00 session=23061 end=1208507728 Exit_status=265 resources_used.cpupercent=0 resources_used.cput=00:00:08 resources_used.mem=41684kb resources_used.ncpus=24 resources_used.vmem=95988kb resources_used.walltime=36:00:47"
        job_ = Job.from_pbs(entry)
        assert_equal(job_.id, "691908.jmayor5.lcrc.anl.gov")
        assert_identical(job_.user, User.fetch("monty"))
        assert_equal(job_.group, "agroup")
        assert_equal(job_.account, Project.fetch("grail"))
        assert_equal(job_.name, "myjob")
        assert_equal(job_.queue, "pri4")
        assert_equal(job_.ctime, datetime.fromtimestamp(1208378066))
        assert_equal(job_.qtime, datetime.fromtimestamp(1208378066))
        assert_equal(job_.etime, datetime.fromtimestamp(1208378066))
        assert_equal(job_.start, datetime.fromtimestamp(1208378066))
        assert_equal(job_.exec_host, "j75/0+j76/0+j77/0+j78/0+j79/0+j86/0+j87/0+j88/0+j89/0+j90/0+j91/0+j93/0+j94/0+j100/0+j101/0+j102/0+j103/0+j104/0+j105/0+j106/0+j107/0+j108/0+j109/0+j110/0")
        assert_equal(job_.resource_list, {'ncpus':24, 'neednodes':24,
            'nodect':24, 'nodes':24, 'walltime':timedelta(hours=36)})
        assert_equal(job_.resources_used, {
            'walltime':timedelta(hours=36, seconds=47),
            'cput':timedelta(seconds=8), 'cpupercent':0, 'vmem':"95988kb",
            'ncpus':24, 'mem':"41684kb"})
        assert_equal(job_.session, 23061)
        assert_equal(job_.end, datetime.fromtimestamp(1208507728))
        assert_equal(job_.exit_status, 265)
    
    def test_from_pbs_e_with_malformed_message (self):
        entry = "04/18/2008 03:35:28;E;691908.jmayor5.lcrc.anl.gov;user=monty group=agroup account=grail jobname=myjob queue=pri4 ctime=1208378066 invalid=value with spaces qtime=1208378066 etime=1208378066 start=1208378066 exec_host=j75/0+j76/0+j77/0+j78/0+j79/0+j86/0+j87/0+j88/0+j89/0+j90/0+j91/0+j93/0+j94/0+j100/0+j101/0+j102/0+j103/0+j104/0+j105/0+j106/0+j107/0+j108/0+j109/0+j110/0 Resource_List.ncpus=24 Resource_List.neednodes=24 Resource_List.nodect=24 Resource_List.nodes=24 Resource_List.walltime=36:00:00 session=23061 end=1208507728 Exit_status=265 resources_used.cpupercent=0 resources_used.cput=00:00:08 resources_used.mem=41684kb resources_used.ncpus=24 resources_used.vmem=95988kb resources_used.walltime=36:00:47"
        job_ = Job.from_pbs(entry)
        assert_equal(job_.id, "691908.jmayor5.lcrc.anl.gov")
        assert_identical(job_.user, User.fetch("monty"))
        assert_equal(job_.group, "agroup")
        assert_equal(job_.account, Project.fetch("grail"))
        assert_equal(job_.name, "myjob")
        assert_equal(job_.queue, "pri4")
        assert_equal(job_.ctime, datetime.fromtimestamp(1208378066))
        assert_equal(job_.qtime, datetime.fromtimestamp(1208378066))
        assert_equal(job_.etime, datetime.fromtimestamp(1208378066))
        assert_equal(job_.start, datetime.fromtimestamp(1208378066))
        assert_equal(job_.exec_host, "j75/0+j76/0+j77/0+j78/0+j79/0+j86/0+j87/0+j88/0+j89/0+j90/0+j91/0+j93/0+j94/0+j100/0+j101/0+j102/0+j103/0+j104/0+j105/0+j106/0+j107/0+j108/0+j109/0+j110/0")
        assert_equal(job_.resource_list, {'ncpus':24, 'neednodes':24,
            'nodect':24, 'nodes':24, 'walltime':timedelta(hours=36)})
        assert_equal(job_.resources_used, {
            'walltime':timedelta(hours=36, seconds=47),
            'cput':timedelta(seconds=8), 'cpupercent':0, 'vmem':"95988kb",
            'ncpus':24, 'mem':"41684kb"})
        assert_equal(job_.session, 23061)
        assert_equal(job_.end, datetime.fromtimestamp(1208507728))
        assert_equal(job_.exit_status, 265)

    def test_str (self):
        job = Job("www.example.com.123")
        assert_equal(str(job), "www.example.com.123")

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


class TestCharge (MappedEntityTester):

    def test_init (self):
        now = datetime.now()
        allocation = Allocation(None, None, 0, None, None)
        charge = Charge(allocation, 600)
        assert_equal(charge.id, None)
        assert_dt_almost_equals(charge.datetime, now)
        assert_identical(charge.allocation, allocation)
        assert_equal(charge.amount, 600)
        assert_equal(charge.comment, None)
        assert_equal(charge.refunds, [])
        assert_equal(charge.job, None)

    def test_persistence (self):
        now = datetime.now()
        user = User.cached("1")
        project = Project.cached("1")
        resource = Resource.cached("1")
        allocation = Allocation(project, resource, 10,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        charge = Charge(allocation, 600)
        charge.comment = "test"
        Session.add(charge)
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        charge = Session.query(Charge).one()
        assert_equal(charge.id, 1)
        assert_dt_almost_equals(charge.datetime, now)
        assert_identical(charge.allocation, allocation)
        assert_equal(charge.amount, 600)
        assert_equal(charge.comment, "test")
        assert_equal(charge.refunds, [])
        assert_equal(charge.job, None)

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
        assert_equal(len(charges), 2)
        assert_equal(sum(charge.amount for charge in charges), 900)
        assert_identical(charges[0].allocation, allocations[0])
        assert_equal(charges[0].amount, 600)
        assert_identical(charges[1].allocation, allocations[1])
        assert_equal(charges[1].amount, 300)

    def test_distributed_zero_amount (self):
        start = datetime.now()
        end = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, end),
            Allocation(None, None, 600, start, end)]
        charges = Charge.distributed(allocations, amount=0)
        assert_equal(len(charges), 1)
        charge = charges[0]
        assert_equal(charge.amount, 0)
        assert_identical(charge.allocation, allocations[0])

    def test_distributed_with_insufficient_allocation (self):
        start = datetime.now()
        end = start + timedelta(days=365)
        allocations = [
            Allocation(None, None, 600, start, end),
            Allocation(None, None, 600, start, end)]
        charges = Charge.distributed(allocations, amount=1300)
        assert_equal(len(charges), 2)
        assert_identical(charges[0].allocation, allocations[0])
        assert_equal(charges[0].amount, 600)
        assert_identical(charges[1].allocation, allocations[1])
        assert_equal(charges[1].amount, 700)

    def test_effective_amount (self):
        charge = Charge(None, 100)
        assert_equal(charge.effective_amount(), 100)

    def test_effective_amount_with_refund (self):
        charge = Charge(None, 100)
        Refund(charge, 10)
        assert_equal(charge.effective_amount(), 90)

    def test_effective_amount_with_multiple_refunds (self):
        charge = Charge(None, 100)
        Refund(charge, 10)
        Refund(charge, 20)
        assert_equal(charge.effective_amount(), 70)


class TestRefund (MappedEntityTester):
    
    def test_init (self):
        now = datetime.now()
        charge = Charge(None, 0)
        refund = Refund(charge, 300)
        assert_equal(refund.id, None)
        assert_dt_almost_equals(refund.datetime, now)
        assert_identical(refund.charge, charge)
        assert_equal(refund.amount, 300)
        assert_equal(refund.comment, None)

    def test_init (self):
        now = datetime.now()
        user = User.cached("1")
        project = Project.cached("1")
        resource = Resource.cached("1")
        allocation = Allocation(project, resource, 10,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        charge = Charge(allocation, 600)
        refund = Refund(charge, 300)
        refund.comment = "test"
        Session.add(refund)
        Session.commit()
        Session.close()
        charge = Session.query(Charge).one()
        refund = Session.query(Refund).one()
        assert_equal(refund.id, 1)
        assert_dt_almost_equals(refund.datetime, now)
        assert_identical(refund.charge, charge)
        assert_equal(refund.amount, 300)
        assert_equal(refund.comment, "test")
    
    def test_full_refund (self):
        charge = Charge(None, 100)
        refund = Refund(charge)
        assert_equal(refund.amount, 100)


def assert_in (item, container):
    assert item in container, "%s not in %s" % (item, container)


def assert_identical (item1, item2):
    assert item1 is item2, "%r is not %r" % (item1, item2)


def assert_not_identical (item1, item2):
    assert item1 is not item2, "%r is %r" % (item1, item2)


def assert_dt_almost_equals (item1, item2, td=timedelta(seconds=1)):
    assert abs(item1 - item2) <= td, "abs(%r - %r) > %r" % (item1, item2, td)
