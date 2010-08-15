from nose.tools import raises, assert_equal

from mock import Mock, patch

from testsuite import (
    BaseTester, setup_upstream, teardown_upstream)

from datetime import datetime, timedelta

import cbank.upstreams.default
from cbank.model.entities import (
    User, Project, Resource, Allocation, Hold, Job, Charge, Refund)
from cbank.model.queries import (
    Session, get_projects, get_users, user_summary)


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


class TestUserSummary (QueryTester):
    
    def test_blank (self):
        users = [User.cached("1"), User.cached("2")]
        assert_equal(list(user_summary(users)), [])
    
    def test_jobs (self):
        user_1 = User.cached("1")
        user_2 = User.cached("2")
        job_1 = Job("1")
        job_2 = Job("2")
        job_3 = Job("3")
        job_4 = Job("4")
        job_1.user_id = "1"
        job_2.user_id = "2"
        job_3.user_id = "2"
        job_4.user_id = "2"
        Session.add_all([job_1, job_2, job_3, job_4])
        assert_equal(list(user_summary([user_1, user_2])),
                     [("1", 1, 0, 0), ("2", 3, 0, 0)])
    
    def test_charges (self):
        user_1 = User.cached("1")
        user_2 = User.cached("2")
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 0, start, end)
        allocation_2 = Allocation(project_2, resource_1, 0, start, end)
        job_1 = Job("1")
        job_2 = Job("2")
        job_3 = Job("3")
        job_4 = Job("4")
        job_1.user_id = "1"
        job_2.user_id = "2"
        job_3.user_id = "2"
        job_4.user_id = "2"
        job_1.charges = [Charge(allocation_1, 1)]
        job_2.charges = [Charge(allocation_1, 2)]
        job_3.charges = [Charge(allocation_2, 4)]
        job_4.charges = [Charge(allocation_2, 8)]
        Session.add_all([allocation_1, allocation_2])
        assert_equal(
            list(user_summary([user_1, user_2])),
            [("1", 1, 1, 0), ("2", 3, 14, 0)])
    
    def test_refunds (self):
        user_1 = User.cached("1")
        user_2 = User.cached("2")
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 0, start, end)
        allocation_2 = Allocation(project_2, resource_1, 0, start, end)
        job_1 = Job("1")
        job_2 = Job("2")
        job_3 = Job("3")
        job_4 = Job("4")
        job_1.user_id = "1"
        job_2.user_id = "2"
        job_3.user_id = "2"
        job_4.user_id = "2"
        job_1.charges = [Charge(allocation_1, 1)]
        job_2.charges = [Charge(allocation_1, 2)]
        job_3.charges = [Charge(allocation_2, 4)]
        job_4.charges = [Charge(allocation_2, 8)]
        Refund(job_1.charges[0], 1)
        Refund(job_2.charges[0], 2)
        Refund(job_3.charges[0], 3)
        Refund(job_4.charges[0], 4)
        Session.add_all([allocation_1, allocation_2])
        assert_equal(
            list(user_summary([user_1, user_2])),
            [("1", 1, 1, 1), ("2", 3, 14, 9)])

    def test_projects_filter (self):
        user_1 = User.cached("1")
        user_2 = User.cached("2")
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 0, start, end)
        allocation_2 = Allocation(project_2, resource_1, 0, start, end)
        job_1 = Job("1")
        job_2 = Job("2")
        job_3 = Job("3")
        job_4 = Job("4")
        job_1.user_id = "1"
        job_2.user_id = "2"
        job_3.user_id = "2"
        job_4.user_id = "2"
        job_1.account_id = "1"
        job_2.account_id = "1"
        job_3.account_id = "2"
        job_4.account_id = "2"
        job_1.charges = [Charge(allocation_1, 1)]
        job_2.charges = [Charge(allocation_1, 2)]
        job_3.charges = [Charge(allocation_2, 4)]
        job_4.charges = [Charge(allocation_2, 8)]
        Refund(job_1.charges[0], 1)
        Refund(job_2.charges[0], 2)
        Refund(job_3.charges[0], 3)
        Refund(job_4.charges[0], 4)
        Session.add_all([
            allocation_1, allocation_2])

        users = [user_1, user_2]
        projects = [Project.cached("1")]
        assert_equal(
            list(user_summary(users, projects=projects)),
            [("1", 1, 1, 1), ("2", 1, 2, 2)])

    def test_resources_filter (self):
        user_1 = User.cached("1")
        user_2 = User.cached("2")
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 0, start, end)
        allocation_2 = Allocation(project_2, resource_2, 0, start, end)
        job_1 = Job("1")
        job_2 = Job("2")
        job_3 = Job("3")
        job_4 = Job("4")
        job_1.user_id = "1"
        job_2.user_id = "2"
        job_3.user_id = "2"
        job_4.user_id = "2"
        job_1.account_id = "1"
        job_2.account_id = "1"
        job_3.account_id = "2"
        job_4.account_id = "2"
        job_1.charges = [Charge(allocation_1, 1)]
        job_2.charges = [Charge(allocation_1, 2)]
        job_3.charges = [Charge(allocation_2, 4)]
        job_4.charges = [Charge(allocation_2, 8)]
        Refund(job_1.charges[0], 1)
        Refund(job_2.charges[0], 2)
        Refund(job_3.charges[0], 3)
        Refund(job_4.charges[0], 4)
        Session.add_all([
            allocation_1, allocation_2])

        users = [user_1, user_2]
        resources = [Resource.cached("2")]
        assert_equal(
            list(user_summary(users, resources=resources)),
            [("1", 0, 0, 0), ("2", 2, 12, 7)])
    
    def test_after_filter (self):
        user_1 = User.cached("1")
        user_2 = User.cached("2")
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 0, start, end)
        allocation_2 = Allocation(project_2, resource_2, 0, start, end)
        job_1 = Job("1")
        job_2 = Job("2")
        job_3 = Job("3")
        job_4 = Job("4")
        job_1.end = datetime(2000, 1, 2)
        job_2.end = datetime(2000, 1, 3)
        job_3.end = datetime(2000, 1, 4)
        job_4.end = datetime(2000, 1, 5)
        job_1.user_id = "1"
        job_2.user_id = "2"
        job_3.user_id = "2"
        job_4.user_id = "2"
        job_1.account_id = "1"
        job_2.account_id = "1"
        job_3.account_id = "2"
        job_4.account_id = "2"
        job_1.charges = [Charge(allocation_1, 1)]
        job_2.charges = [Charge(allocation_1, 2)]
        job_3.charges = [Charge(allocation_2, 4)]
        job_4.charges = [Charge(allocation_2, 8)]
        job_1.charges[0].datetime = datetime(2000, 1, 2)
        job_2.charges[0].datetime = datetime(2000, 1, 3)
        job_3.charges[0].datetime = datetime(2000, 1, 4)
        job_4.charges[0].datetime = datetime(2000, 1, 5)
        Refund(job_1.charges[0], 1)
        Refund(job_2.charges[0], 2)
        Refund(job_3.charges[0], 3)
        Refund(job_4.charges[0], 4)
        Session.add_all([
            allocation_1, allocation_2])

        users = [User.cached("1"), User.cached("2")]
        assert_equal(
            list(user_summary(users, after=datetime(2000, 1, 3))),
            [("1", 0, 0, 0), ("2", 2, 14, 9)])

    def test_before_filter (self):
        user_1 = User.cached("1")
        user_2 = User.cached("2")
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 0, start, end)
        allocation_2 = Allocation(project_2, resource_2, 0, start, end)
        job_1 = Job("1")
        job_2 = Job("2")
        job_3 = Job("3")
        job_4 = Job("4")
        job_1.start = datetime(2000, 1, 1)
        job_2.start = datetime(2000, 1, 2)
        job_3.start = datetime(2000, 1, 3)
        job_4.start = datetime(2000, 1, 4)
        job_1.user_id = "1"
        job_2.user_id = "2"
        job_3.user_id = "2"
        job_4.user_id = "2"
        job_1.account_id = "1"
        job_2.account_id = "1"
        job_3.account_id = "2"
        job_4.account_id = "2"
        job_1.charges = [Charge(allocation_1, 1)]
        job_2.charges = [Charge(allocation_1, 2)]
        job_3.charges = [Charge(allocation_2, 4)]
        job_4.charges = [Charge(allocation_2, 8)]
        job_1.charges[0].datetime = datetime(2000, 1, 2)
        job_2.charges[0].datetime = datetime(2000, 1, 3)
        job_3.charges[0].datetime = datetime(2000, 1, 4)
        job_4.charges[0].datetime = datetime(2000, 1, 5)
        Refund(job_1.charges[0], 1)
        Refund(job_2.charges[0], 2)
        Refund(job_3.charges[0], 3)
        Refund(job_4.charges[0], 4)
        Session.add_all([
            allocation_1, allocation_2])

        users = [User.cached("1"), User.cached("2")]
        assert_equal(
            list(user_summary(users, before=datetime(2000, 1, 3))),
            [("1", 1, 1, 1), ("2", 1, 0, 0)])
