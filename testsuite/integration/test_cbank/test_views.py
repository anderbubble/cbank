from nose.tools import assert_equal

import sys
import os
from datetime import datetime, timedelta
from StringIO import StringIO
from textwrap import dedent

from sqlalchemy import create_engine

import clusterbank
import clusterbank.model
from clusterbank.model import (
    User, Resource, Project, Allocation, Hold,
    Job, Charge, Refund)
from clusterbank.controllers import Session
import clusterbank.model.database
import clusterbank.upstreams.default
import clusterbank.cbank.views
from clusterbank.cbank.views import (
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
    clusterbank.model.database.metadata.bind = create_engine("sqlite:///:memory:")
    clusterbank.model.use_upstream(clusterbank.upstreams.default)
    clusterbank.upstreams.default.projects = [
        clusterbank.upstreams.default.Project("1", "project1"),
        clusterbank.upstreams.default.Project("2", "project2")]
    clusterbank.upstreams.default.resources = [
        clusterbank.upstreams.default.Resource("1", "res1"),
        clusterbank.upstreams.default.Resource("2", "res2")]
    clusterbank.upstreams.default.users = [
        clusterbank.upstreams.default.User(1, "user1"),
        clusterbank.upstreams.default.User(2, "user2")]
    clusterbank.cbank.views.datetime = FakeDateTime(datetime(2000, 1, 1))


def teardown ():
    clusterbank.model.database.metadata.bind = None
    clusterbank.model.clear_upstream()
    clusterbank.upstreams.default.users = []
    clusterbank.upstreams.default.projects = []
    clusterbank.upstreams.default.resources = []
    clusterbank.cbank.views.datetime = datetime


class TestDisplayUnits (object):
    
    def setup (self):
        clusterbank.cbank.views.config.add_section("cbank")
    
    def teardown (self):
        clusterbank.cbank.views.config.remove_section("cbank")
    
    def test_no_unit_factor (self):
        assert_equal(display_units(1000), "1000.0")
    
    def test_unit_factor_simple (self):
        clusterbank.cbank.views.config.set("cbank", "unit_factor", "10")
        assert_equal(display_units(1000), "10000.0")
    
    def test_unit_factor_fraction (self):
        clusterbank.cbank.views.config.set("cbank", "unit_factor", "1/10")
        assert_equal(display_units(1000), "100.0")


class CbankViewTester (object):

    def setup (self):
        clusterbank.model.database.metadata.create_all()
    
    def teardown (self):
        clusterbank.model.database.metadata.drop_all()
        Session.remove()


class TestUsersList (CbankViewTester):
    
    def test_blank (self):
        users = [User.fetch(user) for user in ["user1", "user2"]]
        stdout, stderr = capture(lambda: print_users_list(users))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            user1             0             0.0
            user2             0             0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name           Jobs         Charged
            ---------- -------- ---------------
                       -------- ---------------
                              0             0.0
            Units are undefined.
            """))
    
    def test_jobs (self):
        user1 = User.fetch("user1")
        user2 = User.fetch("user2")
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res1.4")
        j1.user = user1
        j2.user = user2
        j3.user = user2
        j4.user = user2
        Session.add_all([j1, j2, j3, j4])
        Session.flush()
        stdout, stderr = capture(lambda: print_users_list([user1, user2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            user1             1             0.0
            user2             3             0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name           Jobs         Charged
            ---------- -------- ---------------
                       -------- ---------------
                              4             0.0
            Units are undefined.
            """))
    
    def test_charges (self):
        user1 = User.fetch("user1")
        user2 = User.fetch("user2")
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 100, start, end)
        a2 = Allocation(project2, res1, 100, start, end)
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res1.4")
        j1.user = user1
        j2.user = user2
        j3.user = user2
        j4.user = user2
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a1, 7)]
        j3.charges = [Charge(a2, 3)]
        j4.charges = [Charge(a2, 5)]
        Session.add_all([a1, a2])
        Session.flush()
        stdout, stderr = capture(lambda: print_users_list([user1, user2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            user1             1            10.0
            user2             3            15.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name           Jobs         Charged
            ---------- -------- ---------------
                       -------- ---------------
                              4            25.0
            Units are undefined.
            """))
    
    def test_refunds (self):
        user1 = User.fetch("user1")
        user2 = User.fetch("user2")
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 100, start, end)
        a2 = Allocation(project2, res1, 100, start, end)
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res1.4")
        j1.user = user1
        j2.user = user2
        j3.user = user2
        j4.user = user2
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a1, 7)]
        j3.charges = [Charge(a2, 3)]
        j4.charges = [Charge(a2, 5)]
        Refund(j1.charges[0], 9)
        Refund(j2.charges[0], 3)
        Refund(j2.charges[0], 4)
        Refund(j4.charges[0], 3)
        Session.add_all([a1, a2])
        Session.flush()
        stdout, stderr = capture(lambda: print_users_list([user1, user2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            user1             1             1.0
            user2             3             5.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name           Jobs         Charged
            ---------- -------- ---------------
                       -------- ---------------
                              4             6.0
            Units are undefined.
            """))

    def test_projects_filter (self):
        user1 = User.fetch("user1")
        user2 = User.fetch("user2")
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 100, start, end)
        a2 = Allocation(project2, res1, 100, start, end)
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res1.4")
        j1.user = user1
        j2.user = user2
        j3.user = user2
        j4.user = user2
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a1, 7)]
        j3.charges = [Charge(a2, 3)]
        j4.charges = [Charge(a2, 5)]
        Refund(j1.charges[0], 9)
        Refund(j2.charges[0], 3)
        Refund(j2.charges[0], 4)
        Refund(j4.charges[0], 3)
        Session.add_all([a1, a2])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_users_list([user1, user2], projects=[project1]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            user1             0             1.0
            user2             0             0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name           Jobs         Charged
            ---------- -------- ---------------
                       -------- ---------------
                              0             1.0
            Units are undefined.
            """))

    def test_resources_filter (self):
        user1 = User.fetch("user1")
        user2 = User.fetch("user2")
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        res2 = Resource.fetch("res2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 100, start, end)
        a2 = Allocation(project2, res2, 100, start, end)
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res2.1")
        j4 = Job("res2.2")
        j1.user = user1
        j2.user = user2
        j3.user = user2
        j4.user = user2
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a1, 7)]
        j3.charges = [Charge(a2, 3)]
        j4.charges = [Charge(a2, 5)]
        Refund(j1.charges[0], 9)
        Refund(j2.charges[0], 3)
        Refund(j2.charges[0], 4)
        Refund(j4.charges[0], 3)
        Session.add_all([a1, a2])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_users_list([user1, user2], resources=[res2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            user1             0             0.0
            user2             2             5.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name           Jobs         Charged
            ---------- -------- ---------------
                       -------- ---------------
                              2             5.0
            Units are undefined.
            """))
    
    def test_after_filter (self):
        user1 = User.fetch("user1")
        user2 = User.fetch("user2")
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        res2 = Resource.fetch("res2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 100, start, end)
        a2 = Allocation(project2, res2, 100, start, end)
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res1.4")
        j1.user = user1
        j2.user = user2
        j3.user = user2
        j4.user = user2
        j1.end = datetime(2000, 1, 2)
        j2.end = datetime(2000, 1, 3)
        j3.end = datetime(2000, 1, 4)
        j4.end = datetime(2000, 1, 5)
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a1, 7)]
        j3.charges = [Charge(a2, 3)]
        j4.charges = [Charge(a2, 5)]
        j1.charges[0].datetime = datetime(2000, 1, 2)
        j2.charges[0].datetime = datetime(2000, 1, 3)
        j3.charges[0].datetime = datetime(2000, 1, 4)
        j4.charges[0].datetime = datetime(2000, 1, 5)
        Refund(j1.charges[0], 9)
        Refund(j2.charges[0], 3)
        Refund(j2.charges[0], 4)
        Refund(j4.charges[0], 3)
        Session.add_all([a1, a2])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_users_list([user1, user2], after=datetime(2000, 1, 3)))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            user1             0             0.0
            user2             2             5.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name           Jobs         Charged
            ---------- -------- ---------------
                       -------- ---------------
                              2             5.0
            Units are undefined.
            """))

    def test_before_filter (self):
        user1 = User.fetch("user1")
        user2 = User.fetch("user2")
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        res2 = Resource.fetch("res2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 100, start, end)
        a2 = Allocation(project2, res2, 100, start, end)
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res1.4")
        j1.start = datetime(2000, 1, 1)
        j2.start = datetime(2000, 1, 2)
        j3.start = datetime(2000, 1, 3)
        j4.start = datetime(2000, 1, 4)
        j1.user = user1
        j2.user = user2
        j3.user = user2
        j4.user = user2
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a1, 7)]
        j3.charges = [Charge(a2, 3)]
        j4.charges = [Charge(a2, 5)]
        j1.charges[0].datetime = datetime(2000, 1, 2)
        j2.charges[0].datetime = datetime(2000, 1, 3)
        j3.charges[0].datetime = datetime(2000, 1, 4)
        j4.charges[0].datetime = datetime(2000, 1, 5)
        Refund(j1.charges[0], 9)
        Refund(j2.charges[0], 3)
        Refund(j2.charges[0], 4)
        Refund(j4.charges[0], 3)
        Session.add_all([a1, a2])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_users_list([user1, user2], before=datetime(2000, 1, 4)))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            user1             1             1.0
            user2             2             0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name           Jobs         Charged
            ---------- -------- ---------------
                       -------- ---------------
                              3             1.0
            Units are undefined.
            """))


class TestProjectsList (CbankViewTester):
    
    def test_blank (self):
        stdout, stderr = capture(lambda: print_projects_list([]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  0             0.0             0.0
            Units are undefined.
            """))
    
    def test_projects (self):
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        stdout, stderr = capture(lambda:
            print_projects_list([project1, project2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            project1              0             0.0             0.0
            project2              0             0.0             0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  0             0.0             0.0
            Units are undefined.
            """))

    def test_allocations (self):
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
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_projects_list([project1, project2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            project1              0             0.0            30.0
            project2              0             0.0            65.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  0             0.0            95.0
            Units are undefined.
            """))
    
    def test_expired_allocations (self):
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        res2 = Resource.fetch("res2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 10, start, start)
        a2 = Allocation(project1, res1, 20, start, start)
        a3 = Allocation(project2, res1, 30, start, end)
        a4 = Allocation(project2, res2, 35, start, start)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_projects_list([project1, project2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            project1              0             0.0             0.0
            project2              0             0.0            30.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  0             0.0            30.0
            Units are undefined.
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
        Hold(a1, 10)
        Hold(a2, 15)
        a2h1 = Hold(a2, 5)
        a2h1.active = False
        a4h1 = Hold(a4, 9)
        a4h1.active = False
        Hold(a4, 8)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_projects_list([project1, project2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            project1              0             0.0             5.0
            project2              0             0.0            57.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  0             0.0            62.0
            Units are undefined.
            """))
    
    def test_jobs (self):
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        resource1 = Resource.fetch("res1")
        resource2 = Resource.fetch("res2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 0, start, end)
        a2 = Allocation(project2, resource2, 0, start, end)
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res2.1")
        j5 = Job("res2.2")
        c1 = Charge(a1, 0)
        c2 = Charge(a1, 0)
        c3 = Charge(a1, 0)
        c4 = Charge(a2, 0)
        c5 = Charge(a2, 0)
        c1.job = j1
        c2.job = j2
        c3.job = j3
        c4.job = j4
        c5.job = j5
        j1.account = project1
        j2.account = project1
        j3.account = project1
        j4.account = project2
        j5.account = project2
        Session.add_all([j1, j2, j3, j4, j5])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_projects_list([project1, project2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            project1              3             0.0             0.0
            project2              2             0.0             0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  5             0.0             0.0
            Units are undefined.
            """))
    
    
    def test_charges (self):
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
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res2.1")
        j5 = Job("res2.2")
        j1.account = project1
        j2.account = project1
        j3.account = project1
        j4.account = project2
        j5.account = project2
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a2, 15)]
        j3.charges = [Charge(a2, 5)]
        j4.charges = [Charge(a4, 9)]
        j5.charges = [Charge(a4, 8)]
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_projects_list([project1, project2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            project1              3            30.0             0.0
            project2              2            17.0            48.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  5            47.0            48.0
            Units are undefined.
            """))
    
    def test_expired_charges (self):
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        res2 = Resource.fetch("res2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 10, start, start)
        a2 = Allocation(project1, res1, 20, start, end)
        a3 = Allocation(project2, res1, 30, start, start)
        a4 = Allocation(project2, res2, 35, start, start)
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res2.1")
        j5 = Job("res2.2")
        j1.account = project1
        j2.account = project1
        j3.account = project1
        j4.account = project2
        j5.account = project2
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a2, 15)]
        j3.charges = [Charge(a2, 5)]
        j4.charges = [Charge(a4, 9)]
        j5.charges = [Charge(a4, 8)]
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_projects_list([project1, project2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            project1              3            30.0             0.0
            project2              2            17.0             0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  5            47.0             0.0
            Units are undefined.
            """))
    
    def test_refunds (self):
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
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res2.1")
        j5 = Job("res2.2")
        j1.account = project1
        j2.account = project1
        j3.account = project1
        j4.account = project2
        j5.account = project2
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a2, 15)]
        j3.charges = [Charge(a2, 5)]
        j4.charges = [Charge(a4, 9)]
        j5.charges = [Charge(a4, 8)]
        Refund(j1.charges[0], 4)
        Refund(j2.charges[0], 3)
        Refund(j2.charges[0], 5)
        Refund(j5.charges[0], 8)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_projects_list([project1, project2]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            project1              3            18.0            12.0
            project2              2             9.0            56.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  5            27.0            68.0
            Units are undefined.
            """))
    
    def test_after (self):
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
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res2.1")
        j5 = Job("res2.2")
        j1.end = datetime(2000, 1, 2)
        j2.end = datetime(2000, 1, 3)
        j3.end = datetime(2000, 1, 4)
        j4.end = datetime(2000, 1, 2)
        j5.end = datetime(2000, 1, 5)
        j1.account = project1
        j2.account = project1
        j3.account = project1
        j4.account = project2
        j5.account = project2
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a2, 15)]
        j3.charges = [Charge(a2, 5)]
        j4.charges = [Charge(a4, 9)]
        j5.charges = [Charge(a4, 8)]
        j1.charges[0].datetime = datetime(2000, 1, 2)
        j2.charges[0].datetime = datetime(2000, 1, 3)
        j3.charges[0].datetime = datetime(2000, 1, 4)
        j4.charges[0].datetime = datetime(2000, 1, 2)
        j5.charges[0].datetime = datetime(2000, 1, 5)
        Refund(j1.charges[0], 4)
        Refund(j2.charges[0], 3)
        Refund(j2.charges[0], 5)
        Refund(j5.charges[0], 8)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_projects_list([project1, project2],
                after=datetime(2000, 1, 3)))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            project1              1            12.0            12.0
            project2              1             0.0            56.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  2            12.0            68.0
            Units are undefined.
            """))
    
    def test_before (self):
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
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res2.1")
        j5 = Job("res2.2")
        j1.start = datetime(2000, 1, 1)
        j2.start = datetime(2000, 1, 2)
        j3.start = datetime(2000, 1, 3)
        j4.start = datetime(2000, 1, 1)
        j5.start = datetime(2000, 1, 4)
        j1.account = project1
        j2.account = project1
        j3.account = project1
        j4.account = project2
        j5.account = project2
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a2, 15)]
        j3.charges = [Charge(a2, 5)]
        j4.charges = [Charge(a4, 9)]
        j5.charges = [Charge(a4, 8)]
        j1.charges[0].datetime = datetime(2000, 1, 2)
        j2.charges[0].datetime = datetime(2000, 1, 3)
        j3.charges[0].datetime = datetime(2000, 1, 4)
        j4.charges[0].datetime = datetime(2000, 1, 2)
        j5.charges[0].datetime = datetime(2000, 1, 5)
        Refund(j1.charges[0], 4)
        Refund(j2.charges[0], 3)
        Refund(j2.charges[0], 5)
        Refund(j5.charges[0], 8)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_projects_list([project1, project2],
                before=datetime(2000, 1, 3)))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            project1              2             6.0            12.0
            project2              1             9.0            56.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  3            15.0            68.0
            Units are undefined.
            """))

    def test_users (self):
        user1 = User.fetch("user1")
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
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res1.3")
        j4 = Job("res2.1")
        j5 = Job("res2.2")
        j1.user = user1
        j3.user = user1
        j5.user = user1
        j1.account = project1
        j2.account = project1
        j3.account = project1
        j4.account = project2
        j5.account = project2
        j1.charges = [Charge(a1, 10)]
        j2.charges = [Charge(a2, 15)]
        j3.charges = [Charge(a2, 5)]
        j4.charges = [Charge(a4, 9)]
        j5.charges = [Charge(a4, 8)]
        Refund(j1.charges[0], 4)
        Refund(j2.charges[0], 3)
        Refund(j2.charges[0], 5)
        Refund(j5.charges[0], 8)
        Session.add_all([a1, a2, a3, a4])
        stdout, stderr = capture(lambda:
            print_projects_list([project1, project2], users=[user1]))
        stdout_ = dedent("""\
            project1              2            11.0            12.0
            project2              1             0.0            56.0
            """)
        stderr_ = dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  3            11.0            68.0
            Units are undefined.
            """)
        assert_equal_multiline(stdout.getvalue(), stdout_)
        assert_equal_multiline(stderr.getvalue(), stderr_)
    
    def test_resources (self):
        project1 = Project.fetch("project1")
        res1 = Resource.fetch("res1")
        res2 = Resource.fetch("res2")
        a1 = Allocation(project1, res1, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        a2 = Allocation(project1, res2, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        j1 = Job("res1.1")
        j2 = Job("res1.2")
        j3 = Job("res2.1")
        j1.account = project1
        j2.account = project1
        j3.account = project1
        j1.charges = [Charge(a1, 0)]
        j2.charges = [Charge(a1, 0)]
        j3.charges = [Charge(a2, 0)]
        Session.add_all([a1, a2])
        stdout, stderr = capture(lambda:
            print_projects_list([project1], resources=[res1]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            project1              2             0.0             0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            Name               Jobs         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  2             0.0             0.0
            Units are undefined.
            """))


class TestAllocationsList (CbankViewTester):
    
    def test_blank (self):
        stdout, stderr = capture(lambda: print_allocations_list([]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #    End        Resource Project            Jobs       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           0           0.0           0.0
            Units are undefined.
            """))

    def test_upstream_ids (self):
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(Project("-1"), Resource.fetch("-1"), 0, start, end)
        Session.add(a1)
        Session.flush()
        stdout, stderr = capture(lambda: print_allocations_list([a1]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1    2000-01-08 -1       -1                    0           0.0           0.0
            """))
    
    def test_allocations (self):
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
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_allocations_list([a1, a2, a3, a4]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1    2000-01-08 res1     project1              0           0.0          10.0
            2    2000-01-08 res1     project1              0           0.0          20.0
            3    2000-01-08 res1     project2              0           0.0          30.0
            4    2000-01-08 res2     project2              0           0.0          35.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #    End        Resource Project            Jobs       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           0           0.0          95.0
            Units are undefined.
            """))
    
    def test_expired (self):
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        res2 = Resource.fetch("res2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 10, start, start)
        a2 = Allocation(project1, res1, 20, start, end)
        a3 = Allocation(project2, res1, 30, start, end)
        a4 = Allocation(project2, res2, 35, start, start)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_allocations_list([a1, a2, a3, a4]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1    2000-01-01 res1     project1              0           0.0           0.0
            2    2000-01-08 res1     project1              0           0.0          20.0
            3    2000-01-08 res1     project2              0           0.0          30.0
            4    2000-01-01 res2     project2              0           0.0           0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #    End        Resource Project            Jobs       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           0           0.0          50.0
            Units are undefined.
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
        Hold(a1, 10)
        h2 = Hold(a2, 15)
        Hold(a2, 5)
        Hold(a4, 9)
        h5 = Hold(a4, 8)
        h2.active = False
        h5.active = False
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_allocations_list([a1, a2, a3, a4]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1    2000-01-08 res1     project1              0           0.0           0.0
            2    2000-01-08 res1     project1              0           0.0          15.0
            3    2000-01-08 res1     project2              0           0.0          30.0
            4    2000-01-08 res2     project2              0           0.0          26.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #    End        Resource Project            Jobs       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           0           0.0          71.0
            Units are undefined.
            """))
    
    def test_charges (self):
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
        Charge(a1, 10)
        Charge(a2, 15)
        Charge(a2, 5)
        Charge(a4, 9)
        Charge(a4, 8)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_allocations_list([a1, a2, a3, a4]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1    2000-01-08 res1     project1              0          10.0           0.0
            2    2000-01-08 res1     project1              0          20.0           0.0
            3    2000-01-08 res1     project2              0           0.0          30.0
            4    2000-01-08 res2     project2              0          17.0          18.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #    End        Resource Project            Jobs       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           0          47.0          48.0
            Units are undefined.
            """))
    
    def test_expired_charges (self):
        project1 = Project.fetch("project1")
        project2 = Project.fetch("project2")
        res1 = Resource.fetch("res1")
        res2 = Resource.fetch("res2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, res1, 10, start, start)
        a2 = Allocation(project1, res1, 20, start, end)
        a3 = Allocation(project2, res1, 30, start, end)
        a4 = Allocation(project2, res2, 35, start, start)
        Charge(a1, 10)
        Charge(a2, 15)
        Charge(a2, 5)
        Charge(a4, 9)
        Charge(a4, 8)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_allocations_list([a1, a2, a3, a4]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1    2000-01-01 res1     project1              0          10.0           0.0
            2    2000-01-08 res1     project1              0          20.0           0.0
            3    2000-01-08 res1     project2              0           0.0          30.0
            4    2000-01-01 res2     project2              0          17.0           0.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #    End        Resource Project            Jobs       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           0          47.0          30.0
            Units are undefined.
            """))
    
    def test_refunds (self):
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
        Charge(a2, 5)
        Charge(a4, 9)
        c5 = Charge(a4, 8)
        Refund(c1, 4)
        Refund(c2, 3)
        Refund(c2, 5)
        Refund(c5, 8)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_allocations_list([a1, a2, a3, a4]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1    2000-01-08 res1     project1              0           6.0           4.0
            2    2000-01-08 res1     project1              0          12.0           8.0
            3    2000-01-08 res1     project2              0           0.0          30.0
            4    2000-01-08 res2     project2              0           9.0          26.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #    End        Resource Project            Jobs       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           0          27.0          68.0
            Units are undefined.
            """))
    
    def test_jobs (self):
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
        Refund(c1, 4)
        Refund(c2, 3)
        Refund(c2, 5)
        Refund(c5, 8)
        c1.job = Job("res1.1")
        c2.job = Job("res1.2")
        c3.job = Job("res1.3")
        c4.job = Job("res2.1")
        c5.job = Job("res2.2")
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_allocations_list([a1, a2, a3, a4]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1    2000-01-08 res1     project1              1           6.0           4.0
            2    2000-01-08 res1     project1              2          12.0           8.0
            3    2000-01-08 res1     project2              0           0.0          30.0
            4    2000-01-08 res2     project2              2           9.0          26.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #    End        Resource Project            Jobs       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           5          27.0          68.0
            Units are undefined.
            """))

    def test_users_filter (self):
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
        Hold(a4, 9)
        h2 = Hold(a4, 8)
        h2.active = False
        c1 = Charge(a1, 10)
        c2 = Charge(a2, 15)
        c3 = Charge(a2, 5)
        c4 = Charge(a4, 9)
        c5 = Charge(a4, 8)
        Refund(c1, 4)
        Refund(c2, 3)
        Refund(c2, 5)
        Refund(c5, 8)
        c1.job = Job("res1.1")
        c2.job = Job("res1.2")
        c3.job = Job("res1.3")
        c4.job = Job("res2.1")
        c5.job = Job("res2.2")
        c1.job.user = user1
        c2.job.user = user2
        c3.job.user = user1
        c4.job.user = user1
        c5.job.user = user2
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_allocations_list([a1, a2, a3, a4], users=[user1]))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1    2000-01-08 res1     project1              1           6.0           4.0
            2    2000-01-08 res1     project1              1           5.0           8.0
            3    2000-01-08 res1     project2              0           0.0          30.0
            4    2000-01-08 res2     project2              1           9.0          17.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #    End        Resource Project            Jobs       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           3          20.0          59.0
            Units are undefined.
            """))

    def test_after_filter (self):
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
        Hold(a4, 9)
        h2 = Hold(a4, 8)
        h2.active = False
        c1 = Charge(a1, 10)
        c2 = Charge(a2, 15)
        c3 = Charge(a2, 5)
        c4 = Charge(a4, 9)
        c5 = Charge(a4, 8)
        Refund(c1, 4)
        Refund(c2, 3)
        Refund(c2, 5)
        Refund(c5, 8)
        c1.job = Job("res1.1")
        c2.job = Job("res1.2")
        c3.job = Job("res1.3")
        c4.job = Job("res2.1")
        c5.job = Job("res2.2")
        c1.datetime = datetime(2000, 1, 2)
        c2.datetime = datetime(2000, 1, 3)
        c3.datetime = datetime(2000, 1, 4)
        c4.datetime = datetime(2000, 1, 5)
        c5.datetime = datetime(2000, 1, 6)
        c1.job.end = datetime(2000, 1, 2)
        c2.job.end = datetime(2000, 1, 3)
        c3.job.end = datetime(2000, 1, 4)
        c4.job.end = datetime(2000, 1, 5)
        c5.job.end = datetime(2000, 1, 6)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(
            lambda: print_allocations_list(
                [a1, a2, a3, a4], after=datetime(2000, 1, 4)))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1    2000-01-08 res1     project1              0           0.0           4.0
            2    2000-01-08 res1     project1              0           5.0           8.0
            3    2000-01-08 res1     project2              0           0.0          30.0
            4    2000-01-08 res2     project2              2           9.0          17.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #    End        Resource Project            Jobs       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           2          14.0          59.0
            Units are undefined.
            """))

    def test_before_filter (self):
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
        Hold(a4, 9)
        h2 = Hold(a4, 8)
        h2.active = False
        c1 = Charge(a1, 10)
        c2 = Charge(a2, 15)
        c3 = Charge(a2, 5)
        c4 = Charge(a4, 9)
        c5 = Charge(a4, 8)
        Refund(c1, 4)
        Refund(c2, 3)
        Refund(c2, 5)
        Refund(c5, 8)
        c1.job = Job("res1.1")
        c2.job = Job("res1.2")
        c3.job = Job("res1.3")
        c4.job = Job("res2.1")
        c5.job = Job("res2.2")
        c1.datetime = datetime(2000, 1, 2)
        c2.datetime = datetime(2000, 1, 3)
        c3.datetime = datetime(2000, 1, 4)
        c4.datetime = datetime(2000, 1, 5)
        c5.datetime = datetime(2000, 1, 6)
        c1.job.start = datetime(2000, 1, 1)
        c2.job.start = datetime(2000, 1, 2)
        c3.job.start = datetime(2000, 1, 3)
        c4.job.start = datetime(2000, 1, 4)
        c5.job.start = datetime(2000, 1, 5)
        Session.add_all([a1, a2, a3, a4])
        Session.flush()
        stdout, stderr = capture(lambda:
            print_allocations_list([a1, a2, a3, a4],
                                     before=datetime(2000, 1, 4)))
        assert_equal_multiline(stdout.getvalue(), dedent("""\
            1    2000-01-08 res1     project1              1           6.0           4.0
            2    2000-01-08 res1     project1              2           7.0           8.0
            3    2000-01-08 res1     project2              0           0.0          30.0
            4    2000-01-08 res2     project2              0           0.0          17.0
            """))
        assert_equal_multiline(stderr.getvalue(), dedent("""\
            #    End        Resource Project            Jobs       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           3          13.0          59.0
            Units are undefined.
            """))


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
        user1 = User(1)
        user2 = User(2)
        res1 = Resource(1)
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
             * Comment: None
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
             * Comment: None
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
             * Comment: None
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
             * Comment: None
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
             * Comment: None
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
             * Comment: None
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
