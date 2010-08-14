from nose.tools import raises, assert_equal

from mock import Mock, patch

from testsuite import (
    BaseTester, setup_upstream, teardown_upstream)

from datetime import datetime

import cbank.upstreams.default
from cbank.model.entities import (
    User, Project, Resource, Allocation, Hold, Job, Charge, Refund)
from cbank.model.queries import Session, get_projects, get_users


class QueryTester (BaseTester):

    def setup (self):
        self.setup_database()
    
    def teardown (self):
        Session.remove()
        self.teardown_database()


class TestGetProjects (QueryTester):

    def test_no_allocations (self):
        assert_equal(get_projects(), [])

    def test_allocations (self):
        project_1 = Mock(['id'])
        project_1.id = "1"
        project_2 = Mock(['id'])
        project_2.id = "2"
        resource = Mock(['id'])
        resource.id = "1"
        dt = datetime(2000, 1, 1)
        Session.add(Allocation(project_1, resource, 0, dt, dt))
        Session.add(Allocation(project_2, resource, 0, dt, dt))
        assert_equal(
            set(get_projects()),
            set([Project.cached("1"), Project.cached("2")]))

    @patch.object(User, "_member",
                  staticmethod(lambda p, u: p == "1" and u == "1"))
    def test_member_projects (self):
        project_1 = Mock(['id'])
        project_1.id = "1"
        project_2 = Mock(['id'])
        project_2.id = "2"
        resource = Mock(['id'])
        resource.id = "1"
        dt = datetime(2000, 1, 1)
        Session.add(Allocation(project_1, resource, 0, dt, dt))
        Session.add(Allocation(project_2, resource, 0, dt, dt))
        assert_equal(
            get_projects(member=User.cached("1")),
            [Project.cached("1")])

    @patch.object(User, "_manager",
                  staticmethod(lambda p, u: p == "1" and u == "1"))
    def test_manager_projects (self):
        project_1 = Mock(['id'])
        project_1.id = "1"
        project_2 = Mock(['id'])
        project_2.id = "2"
        resource = Mock(['id'])
        resource.id = "1"
        dt = datetime(2000, 1, 1)
        Session.add(Allocation(project_1, resource, 0, dt, dt))
        Session.add(Allocation(project_2, resource, 0, dt, dt))
        assert_equal(
            get_projects(manager=User.cached("1")),
            [Project.cached("1")])


class TestGetUsers (QueryTester):

    def test_no_jobs (self):
        assert_equal(get_users(), [])

    def test_with_jobs (self):
        job_1 = Job("1")
        job_1.user_id = "1"
        job_2 = Job("2")
        job_2.user_id = "2"
        dt = datetime(2000, 1, 1)
        Session.add_all([job_1, job_2])
        assert_equal(
            set(get_users()),
            set([User.cached("1"), User.cached("2")]))

    @patch.object(User, "_member",
                  staticmethod(lambda p, u: p == "1" and u == "1"))
    def test_member_projects (self):
        job_1 = Job("1")
        job_1.user_id = "1"
        job_2 = Job("2")
        job_2.user_id = "2"
        dt = datetime(2000, 1, 1)
        Session.add_all([job_1, job_2])
        assert_equal(
            get_users(member=Project.cached("1")),
            [User.cached("1")])

    @patch.object(User, "_manager",
                  staticmethod(lambda p, u: p == "1" and u == "1"))
    def test_manager_projects (self):
        job_1 = Job("1")
        job_1.user_id = "1"
        job_2 = Job("2")
        job_2.user_id = "2"
        dt = datetime(2000, 1, 1)
        Session.add_all([job_1, job_2])
        assert_equal(
            get_users(manager=Project.cached("1")),
            [User.cached("1")])


class TestPositiveAmountConstraints (QueryTester):

    @raises(ValueError)
    def test_allocation_amount_negative (self):
        a = Allocation(Project("1"), Resource("1"), -1,
                       datetime.now(), datetime.now())
        Session.add(a)
        Session.commit()

    @raises(ValueError)
    def test_hold_amount_negative (self):
        a = Allocation(Project("1"), Resource("1"), 10,
                       datetime.now(), datetime.now())
        h = Hold(a, -1)
        Session.add(h)
        Session.commit()

    @raises(ValueError)
    def test_charge_amount_negative (self):
        a = Allocation(Project("1"), Resource("1"), 10,
                       datetime.now(), datetime.now())
        c = Charge(a, -1)
        Session.add(c)
        Session.commit()

    @raises(ValueError)
    def test_refund_amount_negative (self):
        a = Allocation(Project("1"), Resource("1"), 10,
                       datetime.now(), datetime.now())
        c = Charge(a, 5)
        r = Refund(c, -1)
        Session.add(r)
        Session.commit()


class TestHoldConstraints (QueryTester):

    @raises(ValueError)
    def test_hold_gt (self):
        a = Allocation(Project("1"), Resource("1"), 10,
                       datetime.now(), datetime.now())
        h = Hold(a, 11)
        Session.add(h)
        Session.commit()

    def test_hold_inactive_gt (self):
        a = Allocation(Project("1"), Resource("1"), 10,
                       datetime.now(), datetime.now())
        h = Hold(a, 11)
        h.active = False
        Session.add(h)
        Session.commit()

    def test_hold_inactive_overdrawn (self):
        a = Allocation(Project("1"), Resource("1"), 10,
                       datetime.now(), datetime.now())
        c = Charge(a, 11)
        h = Hold(a, 1)
        h.active = False
        Session.add_all([c, h])
        Session.commit()


class TestRefundConstraints (QueryTester):

    @raises(ValueError)
    def test_refund_gt (self):
        a = Allocation(Project("1"), Resource("1"), 10,
                       datetime.now(), datetime.now())
        c = Charge(a, 5)
        r = Refund(c, 6)
        Session.add(r)
        Session.commit()
