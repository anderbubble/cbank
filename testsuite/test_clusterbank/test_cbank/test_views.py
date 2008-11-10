import sys
import os
from datetime import datetime, timedelta
from StringIO import StringIO
from textwrap import dedent

from sqlalchemy import create_engine

import clusterbank
import clusterbank.model
from clusterbank.model import Allocation, Hold, Charge, Refund
from clusterbank.controllers import user_by_name, project_by_name, \
    resource_by_name, Session
from clusterbank.model.database import metadata
import clusterbank.upstreams.default as upstream
from clusterbank.cbank.views import print_users_report, \
    print_projects_report, print_allocations_report, print_holds_report, \
    print_charges_report


class FakeDateTime (object):
    
    def __init__ (self, now):
        self._now = now
    
    def __call__ (self, *args):
        return datetime(*args)
    
    def now (self):
        return self._now


def setup ():
    metadata.bind = create_engine("sqlite:///:memory:")
    upstream.projects = [
        upstream.Project(1, "project1"), upstream.Project(2, "project2")]
    upstream.resources = [
        upstream.Resource(1, "resource1"), upstream.Resource(2, "resource2")]
    upstream.users = [upstream.User(1, "user1"), upstream.User(2, "user2")]
    clusterbank.model.upstream.use = upstream
    fake_dt = FakeDateTime(datetime(2000, 1, 1))
    clusterbank.cbank.views.datetime = fake_dt


def teardown ():
    upstream.users = []
    upstream.projects = []
    upstream.resources = []
    clusterbank.model.upstream.use = None
    Session.bind = None
    clusterbank.cbank.views.datetime = datetime


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


def assert_eq_output (output, correct):
    assert output == correct, os.linesep.join([
        "incorrect output", output, "expected", correct])


class CbankViewTester (object):

    def setup (self):
        metadata.create_all()
    
    def teardown (self):
        metadata.drop_all()
        Session.remove()


class TestUsersReport (CbankViewTester):
    
    def test_blank (self):
        users = [user_by_name(user) for user in ["user1", "user2"]]
        stdout, stderr = capture(lambda: print_users_report(users))
        correct = dedent("""\
            Name        Charges         Charged
            ---------- -------- ---------------
            user1             0             0.0
            user2             0             0.0
                       -------- ---------------
                              0             0.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_charges (self):
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        user1, user2 = [user_by_name(user) for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1 = resource_by_name("resource1")
        a1 = Allocation(project1, resource1, 100, start, end)
        a2 = Allocation(project2, resource1, 100, start, end)
        Charge(a1, 10).user = user1
        Charge(a1, 7).user = user2
        Charge(a2, 3).user = user2
        Charge(a2, 5).user = user2
        stdout, stderr = capture(lambda: print_users_report([user1, user2]))
        correct = dedent("""\
            Name        Charges         Charged
            ---------- -------- ---------------
            user1             1            10.0
            user2             3            15.0
                       -------- ---------------
                              4            25.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_refunds (self):
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        user1, user2 = [user_by_name(user) for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1 = resource_by_name("resource1")
        a1 = Allocation(project1, resource1, 100, start, end)
        a2 = Allocation(project2, resource1, 100, start, end)
        c1 = Charge(a1, 10)
        c1.user = user1
        Refund(c1, 9)
        c2 = Charge(a1, 7)
        c2.user = user2
        Refund(c2, 3)
        Refund(c2, 4)
        Charge(a2, 3).user = user2
        c4 = Charge(a2, 5)
        c4.user = user2
        Refund(c4, 3)
        stdout, sterr = capture(lambda: print_users_report([user1, user2]))
        correct = dedent("""\
            Name        Charges         Charged
            ---------- -------- ---------------
            user1             1             1.0
            user2             3             5.0
                       -------- ---------------
                              4             6.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_projects_filter (self):
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        user1, user2 = [user_by_name(user) for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1 = resource_by_name("resource1")
        a1 = Allocation(project1, resource1, 100, start, end)
        a2 = Allocation(project2, resource1, 100, start, end)
        c1 = Charge(a1, 10)
        c1.user = user1
        Refund(c1, 9)
        c2 = Charge(a1, 7)
        c2.user = user2
        Refund(c2, 3)
        Refund(c2, 4)
        Charge(a2, 3).user = user2
        c4 = Charge(a2, 5)
        c4.user = user2
        Refund(c4, 3)
        stdout, sterr = capture(lambda:
            print_users_report([user1, user2], projects=[project1]))
        correct = dedent("""\
            Name        Charges         Charged
            ---------- -------- ---------------
            user1             1             1.0
            user2             1             0.0
                       -------- ---------------
                              2             1.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_resources_filter (self):
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        user1, user2 = [user_by_name(user) for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        a1 = Allocation(project1, resource1, 100, start, end)
        a2 = Allocation(project2, resource2, 100, start, end)
        c1 = Charge(a1, 10)
        c1.user = user1
        Refund(c1, 9)
        c2 = Charge(a1, 7)
        c2.user = user2
        Refund(c2, 3)
        Refund(c2, 4)
        Charge(a2, 3).user = user2
        c4 = Charge(a2, 5)
        c4.user = user2
        Refund(c4, 3)
        stdout, sterr = capture(lambda:
            print_users_report([user1, user2], resources=[resource2]))
        correct = dedent("""\
            Name        Charges         Charged
            ---------- -------- ---------------
            user1             0             0.0
            user2             2             5.0
                       -------- ---------------
                              2             5.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_after_filter (self):
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        user1, user2 = [user_by_name(user) for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        a1 = Allocation(project1, resource1, 100, start, end)
        a2 = Allocation(project2, resource2, 100, start, end)
        c1 = Charge(a1, 10)
        c1.datetime = datetime(2000, 1, 2)
        c1.user = user1
        Refund(c1, 9)
        c2 = Charge(a1, 7)
        c2.datetime = datetime(2000, 1, 3)
        c2.user = user2
        Refund(c2, 3)
        Refund(c2, 4)
        c3 = Charge(a2, 3)
        c3.datetime = datetime(2000, 1, 4)
        c3.user = user2
        c4 = Charge(a2, 5)
        c4.datetime = datetime(2000, 1, 5)
        c4.user = user2
        Refund(c4, 3)
        stdout, sterr = capture(lambda:
            print_users_report([user1, user2], after=datetime(2000, 1, 3)))
        correct = dedent("""\
            Name        Charges         Charged
            ---------- -------- ---------------
            user1             0             0.0
            user2             3             5.0
                       -------- ---------------
                              3             5.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_before_filter (self):
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        user1, user2 = [user_by_name(user) for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        a1 = Allocation(project1, resource1, 100, start, end)
        a2 = Allocation(project2, resource2, 100, start, end)
        c1 = Charge(a1, 10)
        c1.datetime = datetime(2000, 1, 2)
        c1.user = user1
        Refund(c1, 9)
        c2 = Charge(a1, 7)
        c2.datetime = datetime(2000, 1, 3)
        c2.user = user2
        Refund(c2, 3)
        Refund(c2, 4)
        c3 = Charge(a2, 3)
        c3.datetime = datetime(2000, 1, 4)
        c3.user = user2
        c4 = Charge(a2, 5)
        c4.datetime = datetime(2000, 1, 5)
        c4.user = user2
        Refund(c4, 3)
        stdout, sterr = capture(lambda:
            print_users_report([user1, user2], before=datetime(2000, 1, 4)))
        correct = dedent("""\
            Name        Charges         Charged
            ---------- -------- ---------------
            user1             1             1.0
            user2             1             0.0
                       -------- ---------------
                              2             1.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)


class TestProjectsReport (CbankViewTester):
    
    def test_blank (self):
        stdout, stderr = capture(lambda: print_projects_report([]))
        correct = dedent("""\
            Name            Charges         Charged       Available
            --------------- ------- --------------- ---------------
                            ------- --------------- ---------------
                                  0             0.0             0.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_projects (self):
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        stdout, stderr = capture(lambda:
            print_projects_report([project1, project2]))
        correct = dedent("""\
            Name            Charges         Charged       Available
            --------------- ------- --------------- ---------------
            project1              0             0.0             0.0
            project2              0             0.0             0.0
                            ------- --------------- ---------------
                                  0             0.0             0.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_allocations (self):
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        Allocation(project1, resource1, 10, start, end)
        Allocation(project1, resource1, 20, start, end)
        Allocation(project2, resource1, 30, start, end)
        Allocation(project2, resource2, 35, start, end)
        stdout, stderr = capture(lambda:
            print_projects_report([project1, project2]))
        correct = dedent("""\
            Name            Charges         Charged       Available
            --------------- ------- --------------- ---------------
            project1              0             0.0            30.0
            project2              0             0.0            65.0
                            ------- --------------- ---------------
                                  0             0.0            95.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_holds (self):
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        Hold(a1, 10)
        a2 = Allocation(project1, resource1, 20, start, end)
        Hold(a2, 15)
        Hold(a2, 5).active = False
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Hold(a4, 9).active = False
        Hold(a4, 8)
        stdout, stderr = capture(lambda:
            print_projects_report([project1, project2]))
        correct = dedent("""\
            Name            Charges         Charged       Available
            --------------- ------- --------------- ---------------
            project1              0             0.0             5.0
            project2              0             0.0            57.0
                            ------- --------------- ---------------
                                  0             0.0            62.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_charges (self):
        user1 = user_by_name("user1")
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        Charge(a1, 10).user = user1
        a2 = Allocation(project1, resource1, 20, start, end)
        Charge(a2, 15).user = user1
        Charge(a2, 5).user = user1
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Charge(a4, 9).user = user1
        Charge(a4, 8).user = user1
        stdout, stderr = capture(lambda:
            print_projects_report([project1, project2]))
        correct = dedent("""\
            Name            Charges         Charged       Available
            --------------- ------- --------------- ---------------
            project1              3            30.0             0.0
            project2              2            17.0            48.0
                            ------- --------------- ---------------
                                  5            47.0            48.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_refunds (self):
        user1 = user_by_name("user1")
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        c1 = Charge(a1, 10)
        c1.user = user1
        Refund(c1, 4)
        a2 = Allocation(project1, resource1, 20, start, end)
        c2 = Charge(a2, 15)
        c2.user = user1
        Refund(c2, 3)
        Refund(c2, 5)
        Charge(a2, 5).user = user1
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Charge(a4, 9).user = user1
        c5 = Charge(a4, 8)
        c5.user = user1
        Refund(c5, 8)
        stdout, stderr = capture(lambda:
            print_projects_report([project1, project2]))
        correct = dedent("""\
            Name            Charges         Charged       Available
            --------------- ------- --------------- ---------------
            project1              3            18.0            12.0
            project2              2             9.0            56.0
                            ------- --------------- ---------------
                                  5            27.0            68.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

class TestAllocationsReport (CbankViewTester):
    
    def test_blank (self):
        stdout, stderr = capture(lambda: print_allocations_report([]))
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           0           0.0           0.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_allocations (self):
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        a2 = Allocation(project1, resource1, 20, start, end)
        a3 = Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Session.flush() # give the allocations ids
        stdout, stderr = capture(lambda:
            print_allocations_report([a1, a2, a3, a4]))
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
            1    2000-01-08 resource1 project1              0           0.0          10.0
            2    2000-01-08 resource1 project1              0           0.0          20.0
            3    2000-01-08 resource1 project2              0           0.0          30.0
            4    2000-01-08 resource2 project2              0           0.0          35.0
                                                     ------- ------------- -------------
                                                           0           0.0          95.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_holds (self):
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        Hold(a1, 10)
        a2 = Allocation(project1, resource1, 20, start, end)
        Hold(a2, 15).active = False
        Hold(a2, 5)
        a3 = Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Hold(a4, 9)
        Hold(a4, 8).active = False
        Session.flush() # give the allocations ids
        stdout, stderr = capture(lambda:
            print_allocations_report([a1, a2, a3, a4]))
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
            1    2000-01-08 resource1 project1              0           0.0           0.0
            2    2000-01-08 resource1 project1              0           0.0          15.0
            3    2000-01-08 resource1 project2              0           0.0          30.0
            4    2000-01-08 resource2 project2              0           0.0          26.0
                                                     ------- ------------- -------------
                                                           0           0.0          71.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_charges (self):
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        Charge(a1, 10)
        a2 = Allocation(project1, resource1, 20, start, end)
        Charge(a2, 15)
        Charge(a2, 5)
        a3 = Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Charge(a4, 9)
        Charge(a4, 8)
        Session.flush() # give the allocations ids
        stdout, stderr = capture(lambda:
            print_allocations_report([a1, a2, a3, a4]))
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
            1    2000-01-08 resource1 project1              1          10.0           0.0
            2    2000-01-08 resource1 project1              2          20.0           0.0
            3    2000-01-08 resource1 project2              0           0.0          30.0
            4    2000-01-08 resource2 project2              2          17.0          18.0
                                                     ------- ------------- -------------
                                                           5          47.0          48.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_refunds (self):
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        c1 = Charge(a1, 10)
        Refund(c1, 4)
        a2 = Allocation(project1, resource1, 20, start, end)
        c2 = Charge(a2, 15)
        Refund(c2, 3)
        Refund(c2, 5)
        Charge(a2, 5)
        a3 = Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Charge(a4, 9)
        c5 = Charge(a4, 8)
        Refund(c5, 8)
        Session.flush() # give the allocations ids
        stdout, stderr = capture(lambda:
            print_allocations_report([a1, a2, a3, a4]))
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
            1    2000-01-08 resource1 project1              1           6.0           4.0
            2    2000-01-08 resource1 project1              2          12.0           8.0
            3    2000-01-08 resource1 project2              0           0.0          30.0
            4    2000-01-08 resource2 project2              2           9.0          26.0
                                                     ------- ------------- -------------
                                                           5          27.0          68.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_users_filter (self):
        user1, user2 = [user_by_name(user)
            for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        c1 = Charge(a1, 10)
        c1.user = user1
        Refund(c1, 4)
        a2 = Allocation(project1, resource1, 20, start, end)
        c2 = Charge(a2, 15)
        c2.user = user2
        Refund(c2, 3)
        Refund(c2, 5)
        c3 = Charge(a2, 5)
        c3.user = user1
        a3 = Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Hold(a4, 9)
        Hold(a4, 8).active = False
        c4 = Charge(a4, 9)
        c4.user = user1
        c5 = Charge(a4, 8)
        c5.user = user2
        Refund(c5, 8)
        Session.flush() # give the allocations ids
        stdout, stderr = capture(lambda:
            print_allocations_report([a1, a2, a3, a4], users=[user1]))
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
            1    2000-01-08 resource1 project1              1           6.0           4.0
            2    2000-01-08 resource1 project1              1           5.0           8.0
            3    2000-01-08 resource1 project2              0           0.0          30.0
            4    2000-01-08 resource2 project2              1           9.0          17.0
                                                     ------- ------------- -------------
                                                           3          20.0          59.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_after_filter (self):
        user1, user2 = [user_by_name(user)
            for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        c1 = Charge(a1, 10)
        c1.datetime = datetime(2000, 1, 1)
        c1.user = user1
        Refund(c1, 4)
        a2 = Allocation(project1, resource1, 20, start, end)
        c2 = Charge(a2, 15)
        c2.datetime = datetime(2000, 1, 2)
        c2.user = user2
        Refund(c2, 3)
        Refund(c2, 5)
        c3 = Charge(a2, 5)
        c3.datetime = datetime(2000, 1, 3)
        c3.user = user1
        a3 = Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Hold(a4, 9)
        Hold(a4, 8).active = False
        c4 = Charge(a4, 9)
        c4.datetime = datetime(2000, 1, 4)
        c4.user = user1
        c5 = Charge(a4, 8)
        c5.datetime = datetime(2000, 1, 5)
        c5.user = user2
        Refund(c5, 8)
        Session.flush() # give the allocations ids
        stdout, stderr = capture(lambda:
            print_allocations_report([a1, a2, a3, a4],
                                     after=datetime(2000, 1, 3)))
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
            1    2000-01-08 resource1 project1              0           0.0           4.0
            2    2000-01-08 resource1 project1              1           5.0           8.0
            3    2000-01-08 resource1 project2              0           0.0          30.0
            4    2000-01-08 resource2 project2              2           9.0          17.0
                                                     ------- ------------- -------------
                                                           3          14.0          59.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_before_filter (self):
        user1, user2 = [user_by_name(user)
            for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        c1 = Charge(a1, 10)
        c1.datetime = datetime(2000, 1, 1)
        c1.user = user1
        Refund(c1, 4)
        a2 = Allocation(project1, resource1, 20, start, end)
        c2 = Charge(a2, 15)
        c2.datetime = datetime(2000, 1, 2)
        c2.user = user2
        Refund(c2, 3)
        Refund(c2, 5)
        c3 = Charge(a2, 5)
        c3.datetime = datetime(2000, 1, 3)
        c3.user = user1
        a3 = Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Hold(a4, 9)
        Hold(a4, 8).active = False
        c4 = Charge(a4, 9)
        c4.datetime = datetime(2000, 1, 4)
        c4.user = user1
        c5 = Charge(a4, 8)
        c5.datetime = datetime(2000, 1, 5)
        c5.user = user2
        Refund(c5, 8)
        Session.flush() # give the allocations ids
        stdout, stderr = capture(lambda:
            print_allocations_report([a1, a2, a3, a4],
                                     before=datetime(2000, 1, 4)))
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
            1    2000-01-08 resource1 project1              1           6.0           4.0
            2    2000-01-08 resource1 project1              2          12.0           8.0
            3    2000-01-08 resource1 project2              0           0.0          30.0
            4    2000-01-08 resource2 project2              0           0.0          17.0
                                                     ------- ------------- -------------
                                                           3          18.0          59.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)


class TestHoldsReport (CbankViewTester):
    
    def test_blank (self):
        stdout, stderr = capture(lambda: print_holds_report([]))
        correct = dedent("""\
            #      Date       Resource Project         User              Held
            ------ ---------- -------- --------------- -------- -------------
                                                                -------------
                                                                          0.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_holds (self):
        user1, user2 = [user_by_name(user)
            for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        h1 = Hold(a1, 10)
        a2 = Allocation(project1, resource1, 20, start, end)
        h2 = Hold(a2, 15)
        h3 = Hold(a2, 5)
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        h4 = Hold(a4, 9)
        h5 = Hold(a4, 8)
        for hold in (h1, h2, h3):
            hold.user = user1
        for hold in (h4, h5):
            hold.user = user2
        for hold in (h1, h2, h3, h4, h5):
            hold.datetime = datetime(2000, 1, 1)
        Session.flush() # give holds ids
        stdout, stderr = capture(lambda:
            print_holds_report([h1, h2, h3, h4, h5]))
        correct = dedent("""\
            #      Date       Resource Project         User              Held
            ------ ---------- -------- --------------- -------- -------------
            1      2000-01-01 resource1 project1        user1             10.0
            2      2000-01-01 resource1 project1        user1             15.0
            3      2000-01-01 resource1 project1        user1              5.0
            4      2000-01-01 resource2 project2        user2              9.0
            5      2000-01-01 resource2 project2        user2              8.0
                                                                -------------
                                                                         47.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)


class TestChargesReport (CbankViewTester):
    
    def test_blank (self):
        stdout, stderr = capture(lambda: print_charges_report([]))
        correct = dedent("""\
            #      Date       Resource Project         User           Charged
            ------ ---------- -------- --------------- -------- -------------
                                                                -------------
                                                                          0.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_charges (self):
        user1, user2 = [user_by_name(user) for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        c1 = Charge(a1, 10)
        a2 = Allocation(project1, resource1, 20, start, end)
        c2 = Charge(a2, 15)
        c3 = Charge(a2, 5)
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        c4 = Charge(a4, 9)
        c5 = Charge(a4, 8)
        for charge in (c1, c2, c3):
            charge.user = user1
        for charge in (c4, c5):
            charge.user = user2
        for charge in (c1, c2, c3, c4, c5):
            charge.datetime = datetime(2000, 1, 1)
        Session.flush() # give charges ids
        stdout, stderr = capture(lambda:
            print_charges_report([c1, c2, c3, c4, c5]))
        correct = dedent("""\
            #      Date       Resource Project         User           Charged
            ------ ---------- -------- --------------- -------- -------------
            1      2000-01-01 resource1 project1        user1             10.0
            2      2000-01-01 resource1 project1        user1             15.0
            3      2000-01-01 resource1 project1        user1              5.0
            4      2000-01-01 resource2 project2        user2              9.0
            5      2000-01-01 resource2 project2        user2              8.0
                                                                -------------
                                                                         47.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_refunds (self):
        user1, user2 = [user_by_name(user) for user in ["user1", "user2"]]
        project1, project2 = [project_by_name(project)
            for project in ["project1", "project2"]]
        resource1, resource2 = [resource_by_name(resource)
            for resource in ["resource1", "resource2"]]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        c1 = Charge(a1, 10)
        Refund(c1, 4)
        a2 = Allocation(project1, resource1, 20, start, end)
        c2 = Charge(a2, 15)
        Refund(c2, 3)
        Refund(c2, 5)
        c3 = Charge(a2, 5)
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        c4 = Charge(a4, 9)
        c5 = Charge(a4, 8)
        Refund(c5, 8)
        for charge in (c1, c2, c3):
            charge.user = user1
        for charge in (c4, c5):
            charge.user = user2
        for charge in (c1, c2, c3, c4, c5):
            charge.datetime = datetime(2000, 1, 1)
        Session.flush() # give charges ids
        stdout, stderr = capture(lambda:
            print_charges_report([c1, c2, c3, c4, c5]))
        correct = dedent("""\
            #      Date       Resource Project         User           Charged
            ------ ---------- -------- --------------- -------- -------------
            1      2000-01-01 resource1 project1        user1              6.0
            2      2000-01-01 resource1 project1        user1              7.0
            3      2000-01-01 resource1 project1        user1              5.0
            4      2000-01-01 resource2 project2        user2              9.0
            5      2000-01-01 resource2 project2        user2              0.0
                                                                -------------
                                                                         27.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

