from nose.tools import assert_equal

import sys
import os
from datetime import datetime, timedelta
from StringIO import StringIO
from textwrap import dedent

from sqlalchemy import create_engine

import cbank
import cbank.model
from cbank.model import (
    User, Resource, Project, Allocation, Hold,
    Job, Charge, Refund,
    Session)
import cbank.model.database
import cbank.upstreams.volatile
import cbank.cli.views
from cbank.cli.views import (
    print_users_list, print_projects_list, print_allocations_list,
    print_holds_list, print_jobs_list, print_charges_list,
    print_charges, print_jobs, print_refunds, print_holds, display_units)


class FakeDateTime (object):
    
    def __init__ (self, now):
        self._now = now
    
    def __call__ (self, *args):
        return datetime(*args)
    
    def now (self):
        return self._now


def setup ():
    cbank.model.database.metadata.bind = create_engine("sqlite:///:memory:")
    cbank.model.use_upstream(cbank.upstreams.volatile)
    cbank.upstreams.volatile.projects = [
        cbank.upstreams.volatile.Project("1", "project1"),
        cbank.upstreams.volatile.Project("2", "project2")]
    cbank.upstreams.volatile.resources = [
        cbank.upstreams.volatile.Resource("1", "res1"),
        cbank.upstreams.volatile.Resource("2", "res2")]
    cbank.upstreams.volatile.users = [
        cbank.upstreams.volatile.User("1", "user1"),
        cbank.upstreams.volatile.User("2", "user2")]
    datetime_ = FakeDateTime(datetime(2000, 1, 1))
    cbank.model.entities.Allocation.active.im_func.func_defaults = (datetime(2000, 1, 1), )
    cbank.model.queries.datetime = datetime_
    cbank.cli.views.datetime = datetime_


def teardown ():
    cbank.model.database.metadata.bind = None
    cbank.model.use_upstream(None)
    cbank.upstreams.volatile.users = []
    cbank.upstreams.volatile.projects = []
    cbank.upstreams.volatile.resources = []
    cbank.model.entities.Allocation.active.im_func.func_defaults = (datetime.now, )
    cbank.model.queries.datetime = datetime
    cbank.cli.views.datetime = datetime


class TestDisplayUnits (object):
    
    def setup (self):
        cbank.cli.views.config.add_section("cli")
    
    def teardown (self):
        cbank.cli.views.config.remove_section("cli")
    
    def test_no_unit_factor (self):
        assert_equal(display_units(1000), "1000.0")
    
    def test_unit_factor_simple (self):
        cbank.cli.views.config.set("cli", "unit_factor", "10")
        assert_equal(display_units(1000), "10000.0")
    
    def test_unit_factor_fraction (self):
        cbank.cli.views.config.set("cli", "unit_factor", "1/10")
        assert_equal(display_units(1000), "100.0")


class CbankViewTester (object):

    def setup (self):
        cbank.model.database.metadata.create_all()
    
    def teardown (self):
        Session.remove()
        cbank.model.database.metadata.drop_all()


class TestHoldsList (CbankViewTester):
    
    def test_blank (self):
        stdout, stderr = capture(lambda: print_holds_list([]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #      Date       Resource Project                  Held
            ------ ---------- -------- --------------- -------------
                                                       -------------
                                                                 0.0
            Units are undefined.
            """))

    def test_upstream_ids (self):
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(Project("-1"), Resource.fetch("-1"), 0, start, end)
        h1 = Hold(a1, 0)
        h1.datetime = datetime(2000, 1, 1)
        Session.add(h1)
        Session.flush()
        stdout, stderr = capture(lambda:
            print_holds_list([h1]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1      2000-01-01 -1       -1                        0.0
            """))
    
    def test_holds (self):
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        res2 = Resource.fetch("res2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 10, start, end)
        a2 = Allocation(project1, res1, 20, start, end)
        a3 = Allocation(project2, res1, 30, start, end)
        a4 = Allocation(project2, res2, 35, start, end)
        h1 = Hold(a1, 10)
        h2 = Hold(a2, 15)
        h3 = Hold(a2, 5)
        h4 = Hold(a4, 9)
        h5 = Hold(a4, 8)
        for hold in (h1, h2, h3, h4, h5):
            hold.datetime = datetime(2000, 1, 1)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_holds_list([h1, h2, h3, h4, h5]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1      2000-01-01 res1     project1                 10.0
            2      2000-01-01 res1     project1                 15.0
            3      2000-01-01 res1     project1                  5.0
            4      2000-01-01 res2     project2                  9.0
            5      2000-01-01 res2     project2                  8.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #      Date       Resource Project                  Held
            ------ ---------- -------- --------------- -------------
                                                       -------------
                                                                47.0
            Units are undefined.
            """))


class TestJobsList (CbankViewTester):
    
    def setup (self):
        CbankViewTester.setup(self)
    
    def teardown (self):
        CbankViewTester.teardown(self)
    
    def test_blank (self):
        stdout, stderr = capture(lambda: print_jobs_list([]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            ID                  Name       User     Account          Duration       Charged
            ------------------- ---------- -------- --------------- --------- -------------
                                                                    --------- -------------
                                                                      0:00:00           0.0
            Units are undefined.
            """))
    
    def test_upstream_ids (self):
        s = Session()
        job = Job("res1.1")
        job.account = Project("-1")
        s.add(job)
        stdout, stderr = capture(lambda: print_jobs_list([job]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            res1.1                                  -1                                  0.0
            """))
    
    def test_bare_jobs (self):
        s = Session()
        jobs = [Job("res1.1"), Job("res1.2"), Job("res1.3")]
        for job in jobs:
            s.add(job)
        stdout, stderr = capture(lambda: print_jobs_list(jobs))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            res1.1                                                                      0.0
            res1.2                                                                      0.0
            res1.3                                                                      0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            ID                  Name       User     Account          Duration       Charged
            ------------------- ---------- -------- --------------- --------- -------------
                                                                    --------- -------------
                                                                      0:00:00           0.0
            Units are undefined.
            """))
    
    def test_long_job (self):
        s = Session()
        job = Job("res1.1")
        job.start = datetime(2000, 1, 1)
        job.end = datetime(2000, 2, 1)
        s.add(job)
        stdout, stderr = capture(lambda: print_jobs_list([job]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            res1.1                                                  744:00:00           0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            ID                  Name       User     Account          Duration       Charged
            ------------------- ---------- -------- --------------- --------- -------------
                                                                    --------- -------------
                                                                    744:00:00           0.0
            Units are undefined.
            """))
    
    def test_full_jobs (self):
        s = Session()
        project1 = Project("1")
        project2 = Project("2")
        user1 = User("1")
        user2 = User("2")
        res1 = Resource("1")
        a = Allocation(project1, res1, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j2.user = user1
        j3.user = user2
        j1.account = project1
        j3.account = project2
        j1.name = "somename"
        j1.charges = [Charge(a, 10), Charge(a, 20)]
        j1.charges[1].refund(5)
        j1.start = datetime(2000, 1, 1)
        j2.start = datetime(2000, 1, 2)
        j1.end = j1.start + timedelta(minutes=30)
        for job in [j1, j2, j3]:
            s.add(job)
        stdout, stderr = capture(lambda: print_jobs_list([j1, j2, j3]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            res1.1              somename            project1          0:30:00          25.0
            res1.2                         user1                                        0.0
            res1.3                         user2    project2                            0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            ID                  Name       User     Account          Duration       Charged
            ------------------- ---------- -------- --------------- --------- -------------
                                                                    --------- -------------
                                                                      0:30:00          25.0
            Units are undefined.
            """))


class TestChargesList (CbankViewTester):
    
    def test_blank (self):
        stdout, stderr = capture(lambda: print_charges_list([]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #      Date       Resource Project               Charged
            ------ ---------- -------- --------------- -------------
                                                       -------------
                                                                 0.0
            Units are undefined.
            """))

    def test_upstream_ids (self):
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(Project("-1"), Resource.fetch("-1"), 0, start, end)
        c1 = Charge(a1, 0)
        c1.datetime = datetime(2000, 1, 1)
        Session.add(c1)
        Session.flush()
        stdout, stderr = capture(lambda:
            print_charges_list([c1]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1      2000-01-01 -1       -1                        0.0
            """))
    
    def test_charges (self):
        user1 = User.fetch("user1")
        user2 = User.fetch("user2")
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        res2 = Resource.fetch("res2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 10, start, end)
        a2 = Allocation(project1, res1, 20, start, end)
        a3 = Allocation(project2, res1, 30, start, end)
        a4 = Allocation(project2, res2, 35, start, end)
        c1 = Charge(a1, 10)
        c2 = Charge(a2, 15)
        c3 = Charge(a2, 5)
        c4 = Charge(a4, 9)
        c5 = Charge(a4, 8)
        for charge in (c1, c2, c3, c4, c5):
            charge.datetime = datetime(2000, 1, 1)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_charges_list([c1, c2, c3, c4, c5]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1      2000-01-01 res1     project1                 10.0
            2      2000-01-01 res1     project1                 15.0
            3      2000-01-01 res1     project1                  5.0
            4      2000-01-01 res2     project2                  9.0
            5      2000-01-01 res2     project2                  8.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #      Date       Resource Project               Charged
            ------ ---------- -------- --------------- -------------
                                                       -------------
                                                                47.0
            Units are undefined.
            """))
    
    def test_refunds (self):
        user1 = User.fetch("user1")
        user2 = User.fetch("user2")
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        res2 = Resource.fetch("res2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 10, start, end)
        a2 = Allocation(project1, res1, 20, start, end)
        a3 = Allocation(project2, res1, 30, start, end)
        a4 = Allocation(project2, res2, 35, start, end)
        c1 = Charge(a1, 10)
        c2 = Charge(a2, 15)
        c3 = Charge(a2, 5)
        c4 = Charge(a4, 9)
        c5 = Charge(a4, 8)
        for charge in (c1, c2, c3, c4, c5):
            charge.datetime = datetime(2000, 1, 1)
        Refund(c1, 4)
        Refund(c2, 3)
        Refund(c2, 5)
        Refund(c5, 8)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_charges_list([c1, c2, c3, c4, c5]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1      2000-01-01 res1     project1                  6.0
            2      2000-01-01 res1     project1                  7.0
            3      2000-01-01 res1     project1                  5.0
            4      2000-01-01 res2     project2                  9.0
            5      2000-01-01 res2     project2                  0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #      Date       Resource Project               Charged
            ------ ---------- -------- --------------- -------------
                                                       -------------
                                                                27.0
            Units are undefined.
            """))


class TestPrintHolds (CbankViewTester):
    
    def test_hold (self):
        user1 = User.fetch("user1")
        project1 = Project.fetch("project1")
        res1 = Resource.fetch("res1")
        allocation1 = Allocation(project1, res1, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        hold = Hold(allocation1, 0)
        hold.datetime = datetime(2000, 1, 1)
        Session.add(hold)
        Session.flush()
        stdout, stderr = capture(lambda:
            print_holds([hold]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            Hold 1 -- 0.0
             * Datetime: 2000-01-01 00:00:00
             * Active: True
             * Allocation: 1
             * Project: project1
             * Resource: res1
             * Comment: 
             * Job: None
            """))
    
    def test_job_hold (self):
        user1 = User.fetch("user1")
        project1 = Project.fetch("project1")
        res1 = Resource.fetch("res1")
        allocation1 = Allocation(project1, res1, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        hold = Hold(allocation1, 0)
        hold.datetime = datetime(2000, 1, 1)
        hold.job = Job("res1.1")
        Session.add(hold)
        Session.flush()
        stdout, stderr = capture(lambda:
            print_holds([hold]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            Hold 1 -- 0.0
             * Datetime: 2000-01-01 00:00:00
             * Active: True
             * Allocation: 1
             * Project: project1
             * Resource: res1
             * Comment: 
             * Job: res1.1
            """))


class TestPrintJobs (CbankViewTester):
    
    def test_job (self):
        user1 = User.fetch("user1")
        project1 = Project.fetch("project1")
        res1 = Resource.fetch("res1")
        allocation1 = Allocation(project1, res1, 0,
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
        Session.add(job)
        Session.flush()
        stdout, stderr = capture(lambda:
            print_jobs([job]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            Job www.example.com.123
             * User: user1
             * Group: agroup
             * Account: project1
             * Name: myjob
             * Queue: aqueue
             * Reservation name: areservation
             * Reservation id: www.example.com.1
             * Creation time: 2000-01-01 00:00:00
             * Queue time: 2001-01-01 00:00:00
             * Eligible time: 2001-01-02 00:00:00
             * Start: 2001-02-02 00:00:00
             * Execution host: ANL-R00-M1-512
             * Resource list:
                * nodes: 64
                * otherresource: stringvalue
                * walltime: 0:10:00
             * Session: 123
             * Alternate id: anotherid
             * End: 2001-02-03 00:00:00
             * Exit status: 128
             * Resources used:
                * nodes: 64
                * otherresource: stringvalue
                * walltime: 0:10:00
             * Accounting id: someaccountingid
            """))


class TestPrintCharges (CbankViewTester):
    
    def test_charge (self):
        user1 = User.fetch("user1")
        project1 = Project.fetch("project1")
        res1 = Resource.fetch("res1")
        allocation1 = Allocation(project1, res1, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        charge = Charge(allocation1, 0)
        charge.datetime = datetime(2000, 1, 1)
        Session.add(charge)
        Session.flush()
        stdout, stderr = capture(lambda:
            print_charges([charge]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            Charge 1 -- 0.0
             * Datetime: 2000-01-01 00:00:00
             * Allocation: 1
             * Project: project1
             * Resource: res1
             * Comment: 
             * Job: None
            """))
    
    def test_job_charge (self):
        user1 = User.fetch("user1")
        project1 = Project.fetch("project1")
        res1 = Resource.fetch("res1")
        allocation1 = Allocation(project1, res1, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        charge = Charge(allocation1, 0)
        charge.datetime = datetime(2000, 1, 1)
        charge.job = Job("res1.1")
        Session.add(charge)
        Session.flush()
        stdout, stderr = capture(lambda:
            print_charges([charge]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            Charge 1 -- 0.0
             * Datetime: 2000-01-01 00:00:00
             * Allocation: 1
             * Project: project1
             * Resource: res1
             * Comment: 
             * Job: res1.1
            """))


class TestPrintRefunds (CbankViewTester):
    
    def test_refund (self):
        user1 = User.fetch("user1")
        project1 = Project.fetch("project1")
        res1 = Resource.fetch("res1")
        allocation1 = Allocation(project1, res1, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        charge = Charge(allocation1, 0)
        refund = Refund(charge, 0)
        refund.datetime = datetime(2000, 1, 1)
        Session.add(refund)
        Session.flush()
        stdout, stderr = capture(lambda:
            print_refunds([refund]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            Refund 1 -- 0.0
             * Datetime: 2000-01-01 00:00:00
             * Charge: 1
             * Allocation: 1
             * Project: project1
             * Resource: res1
             * Comment: 
             * Job: None
            """))
    
    def test_job_refund (self):
        user1 = User.fetch("user1")
        project1 = Project.fetch("project1")
        res1 = Resource.fetch("res1")
        allocation1 = Allocation(project1, res1, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        charge = Charge(allocation1, 0)
        charge.job = Job("res1.1")
        refund = Refund(charge, 0)
        refund.datetime = datetime(2000, 1, 1)
        Session.add(refund)
        Session.flush()
        stdout, stderr = capture(lambda:
            print_refunds([refund]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            Refund 1 -- 0.0
             * Datetime: 2000-01-01 00:00:00
             * Charge: 1
             * Allocation: 1
             * Project: project1
             * Resource: res1
             * Comment: 
             * Job: res1.1
            """))


def capture (func):
    real_stdout = sys.stdout
    real_stderr = sys.stderr
    try:
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        func()
        for f in (sys.stdout, sys.stderr):
            f.flush()
            f.seek(0)
        return sys.stdout, sys.stderr
    finally:
        sys.stdout = real_stdout
        sys.stderr = real_stderr


def assert_equal_multiline (output, correct):
    assert output == correct, os.linesep.join([
        "incorrect output", output, "expected", correct])
