from nose.tools import raises, assert_equal

from mock import Mock, patch

from testsuite import (
    BaseTester, setup_upstream, teardown_upstream)

from datetime import datetime, timedelta

from cbank.model.entities import (
    User, Project, Resource, Allocation, Hold, Job, Charge, Refund)
from cbank.model.queries import (
    Session, get_projects, get_users,
    user_summary, project_summary, allocation_summary)


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
                     [("1", 1, 0), ("2", 3, 0)])
    
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
            [("1", 1, 1), ("2", 3, 14)])
    
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
            [("1", 1, 0), ("2", 3, 5)])

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
            [("1", 1, 0), ("2", 1, 0)])

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
            [("1", 0, 0), ("2", 2, 5)])
    
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
            [("1", 0, 0), ("2", 2, 5)])

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
            [("1", 1, 0), ("2", 1, 0)])


class TestProjectSummary (QueryTester):

    datetime_mock = Mock(['now'])
    datetime_mock.now = Mock([], return_value=datetime(2000, 1, 1))

    def test_projects (self):
        projects = [Project.cached("1"), Project.cached("2")]
        assert_equal(list(project_summary(projects)), [])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_allocations (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        Session.add_all([
            allocation_1, allocation_2, allocation_3, allocation_4])
        assert_equal(list(project_summary([project_1, project_2])),
                     [('1', 0, 0, 30), ("2", 0, 0, 65)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_expired_allocations (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, start)
        allocation_2 = Allocation(project_1, resource_1, 20, start, start)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, start)
        Session.add_all([
            allocation_1, allocation_2, allocation_3, allocation_4])
        assert_equal(list(project_summary([project_1, project_2])),
                     [("1", 0, 0, 0), ("2", 0, 0, 30)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_holds (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, start)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        Hold(allocation_1, 10)
        Hold(allocation_2, 15)
        hold_3 = Hold(allocation_2, 5)
        hold_4 = Hold(allocation_4, 9)
        Hold(allocation_4, 8)
        hold_3.active = False
        hold_4.active = False
        Session.add_all([
            allocation_1, allocation_2, allocation_3, allocation_4])
        assert_equal(list(project_summary([project_1, project_2])),
                     [("1", 0, 0, 5), ("2", 0, 0, 57)])

    def test_jobs (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("1")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 0, start, end)
        allocation_2 = Allocation(project_2, resource_2, 0, start, end)
        job_1 = Job("1.1")
        job_2 = Job("1.2")
        job_3 = Job("1.3")
        job_4 = Job("2.1")
        job_5 = Job("2.2")
        charge_1 = Charge(allocation_1, 0)
        charge_2 = Charge(allocation_1, 0)
        charge_3 = Charge(allocation_1, 0)
        charge_4 = Charge(allocation_2, 0)
        charge_5 = Charge(allocation_2, 0)
        charge_1.job = job_1
        charge_2.job = job_2
        charge_3.job = job_3
        charge_4.job = job_4
        charge_5.job = job_5
        job_1.account = project_1
        job_2.account = project_1
        job_3.account = project_1
        job_4.account = project_2
        job_5.account = project_2
        Session.add_all([job_1, job_2, job_3, job_4, job_5])
        assert_equal(list(project_summary([project_1, project_2])),
                     [("1", 3, 0, 0), ("2", 2, 0, 0)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_charges (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        job_1 = Job("1.1")
        job_2 = Job("1.2")
        job_3 = Job("1.3")
        job_4 = Job("2.1")
        job_5 = Job("2.2")
        job_1.account = project_1
        job_2.account = project_1
        job_3.account = project_1
        job_4.account = project_2
        job_5.account = project_2
        job_1.charges = [Charge(allocation_1, 10)]
        job_2.charges = [Charge(allocation_2, 15)]
        job_3.charges = [Charge(allocation_2, 5)]
        job_4.charges = [Charge(allocation_4, 9)]
        job_5.charges = [Charge(allocation_4, 8)]
        Session.add_all([allocation_1, allocation_2, allocation_3, allocation_4])
        assert_equal(list(project_summary([project_1, project_2])),
                     [("1", 3, 30, 0), ("2", 2, 17, 48)])
    
    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_expired_charges (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, start)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, start)
        allocation_4 = Allocation(project_2, resource_2, 35, start, start)
        job_1 = Job("1.1")
        job_2 = Job("1.2")
        job_3 = Job("1.3")
        job_4 = Job("2.1")
        job_5 = Job("2.2")
        job_1.account = project_1
        job_2.account = project_1
        job_3.account = project_1
        job_4.account = project_2
        job_5.account = project_2
        job_1.charges = [Charge(allocation_1, 10)]
        job_2.charges = [Charge(allocation_2, 15)]
        job_3.charges = [Charge(allocation_2, 5)]
        job_4.charges = [Charge(allocation_4, 9)]
        job_5.charges = [Charge(allocation_4, 8)]
        Session.add_all([allocation_1, allocation_2, allocation_3, allocation_4])
        assert_equal(list(project_summary([project_1, project_2])),
                     [("1", 3, 30, 0), ("2", 2, 17, 0)])
    
    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_refunds (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        job_1 = Job("1.1")
        job_2 = Job("1.2")
        job_3 = Job("1.3")
        job_4 = Job("2.1")
        job_5 = Job("2.2")
        job_1.account = project_1
        job_2.account = project_1
        job_3.account = project_1
        job_4.account = project_2
        job_5.account = project_2
        job_1.charges = [Charge(allocation_1, 10)]
        job_2.charges = [Charge(allocation_2, 15)]
        job_3.charges = [Charge(allocation_2, 5)]
        job_4.charges = [Charge(allocation_4, 9)]
        job_5.charges = [Charge(allocation_4, 8)]
        Refund(job_1.charges[0], 4)
        Refund(job_2.charges[0], 3)
        Refund(job_2.charges[0], 5)
        Refund(job_5.charges[0], 8)
        Session.add_all([allocation_1, allocation_2, allocation_3, allocation_4])
        assert_equal(list(project_summary([project_1, project_2])),
                     [("1", 3, 18, 12), ("2", 2, 9, 56)])
    
    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_after (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        job_1 = Job("1.1")
        job_2 = Job("1.2")
        job_3 = Job("1.3")
        job_4 = Job("2.1")
        job_5 = Job("2.2")
        job_1.account = project_1
        job_2.account = project_1
        job_3.account = project_1
        job_4.account = project_2
        job_5.account = project_2
        job_1.charges = [Charge(allocation_1, 10)]
        job_2.charges = [Charge(allocation_2, 15)]
        job_3.charges = [Charge(allocation_2, 5)]
        job_4.charges = [Charge(allocation_4, 9)]
        job_5.charges = [Charge(allocation_4, 8)]
        Refund(job_1.charges[0], 4)
        Refund(job_2.charges[0], 3)
        Refund(job_2.charges[0], 5)
        Refund(job_5.charges[0], 8)
        job_1.end = datetime(2000, 1, 2)
        job_2.end = datetime(2000, 1, 3)
        job_3.end = datetime(2000, 1, 4)
        job_4.end = datetime(2000, 1, 2)
        job_5.end = datetime(2000, 1, 5)
        job_1.charges[0].datetime = datetime(2000, 1, 2)
        job_2.charges[0].datetime = datetime(2000, 1, 3)
        job_3.charges[0].datetime = datetime(2000, 1, 4)
        job_4.charges[0].datetime = datetime(2000, 1, 2)
        job_5.charges[0].datetime = datetime(2000, 1, 5)
        Session.add_all([allocation_1, allocation_2, allocation_3, allocation_4])
        assert_equal(
            list(project_summary([project_1, project_2], after=datetime(2000, 1, 3))),
            [("1", 1, 12, 12), ("2", 1, 0, 56)])
    
    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_before (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        job_1 = Job("1.1")
        job_2 = Job("1.2")
        job_3 = Job("1.3")
        job_4 = Job("2.1")
        job_5 = Job("2.2")
        job_1.account = project_1
        job_2.account = project_1
        job_3.account = project_1
        job_4.account = project_2
        job_5.account = project_2
        job_1.charges = [Charge(allocation_1, 10)]
        job_2.charges = [Charge(allocation_2, 15)]
        job_3.charges = [Charge(allocation_2, 5)]
        job_4.charges = [Charge(allocation_4, 9)]
        job_5.charges = [Charge(allocation_4, 8)]
        Refund(job_1.charges[0], 4)
        Refund(job_2.charges[0], 3)
        Refund(job_2.charges[0], 5)
        Refund(job_5.charges[0], 8)
        job_1.start = datetime(2000, 1, 1)
        job_2.start = datetime(2000, 1, 2)
        job_3.start = datetime(2000, 1, 3)
        job_4.start = datetime(2000, 1, 1)
        job_5.start = datetime(2000, 1, 4)
        job_1.charges[0].datetime = datetime(2000, 1, 2)
        job_2.charges[0].datetime = datetime(2000, 1, 3)
        job_3.charges[0].datetime = datetime(2000, 1, 4)
        job_4.charges[0].datetime = datetime(2000, 1, 2)
        job_5.charges[0].datetime = datetime(2000, 1, 5)
        Session.add_all([allocation_1, allocation_2, allocation_3, allocation_4])
        assert_equal(
            list(project_summary([project_1, project_2], before=datetime(2000, 1, 3))),
            [("1", 2, 6, 12), ("2", 1, 9, 56)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_users (self):
        user_1 = User.cached("1")
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        job_1 = Job("1.1")
        job_2 = Job("1.2")
        job_3 = Job("1.3")
        job_4 = Job("2.1")
        job_5 = Job("2.2")
        job_1.user = user_1
        job_3.user = user_1
        job_5.user = user_1
        job_1.account = project_1
        job_2.account = project_1
        job_3.account = project_1
        job_4.account = project_2
        job_5.account = project_2
        job_1.charges = [Charge(allocation_1, 10)]
        job_2.charges = [Charge(allocation_2, 15)]
        job_3.charges = [Charge(allocation_2, 5)]
        job_4.charges = [Charge(allocation_4, 9)]
        job_5.charges = [Charge(allocation_4, 8)]
        Refund(job_1.charges[0], 4)
        Refund(job_2.charges[0], 3)
        Refund(job_2.charges[0], 5)
        Refund(job_5.charges[0], 8)
        Session.add_all([allocation_1, allocation_2, allocation_3, allocation_4])
        assert_equal(
            list(project_summary([project_1, project_2], users=[user_1])),
            [("1", 2, 11, 12), ("2", 1, 0, 56)])
    
    def test_resources (self):
        project_1 = Project.cached("1")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        allocation_1 = Allocation(project_1, resource_1, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation_2 = Allocation(project_1, resource_2, 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        job_1 = Job("1.1")
        job_2 = Job("1.2")
        job_3 = Job("2.1")
        job_1.account = project_1
        job_2.account = project_1
        job_3.account = project_1
        job_1.charges = [Charge(allocation_1, 0)]
        job_2.charges = [Charge(allocation_1, 0)]
        job_3.charges = [Charge(allocation_2, 0)]
        Session.add_all([allocation_1, allocation_2])
        assert_equal(
            list(project_summary([project_1], resources=[resource_1])),
            [("1", 2, 0, 0)])


class TestAllocationSummary (QueryTester):

    datetime_mock = Mock(['now'])
    datetime_mock.now = Mock([], return_value=datetime(2000, 1, 1))

    def test_blank (self):
        assert_equal(list(allocation_summary([])), [])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_allocations (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        allocations = [allocation_1, allocation_2, allocation_3, allocation_4]
        Session.add_all(allocations)
        Session.flush()
        assert_equal(
            list(allocation_summary(allocations)),
            [(allocation_1, 0, 0, 10),
             (allocation_2, 0, 0, 20),
             (allocation_3, 0, 0, 30),
             (allocation_4, 0, 0, 35)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_expired (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, start)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, start)
        allocations = [allocation_1, allocation_2, allocation_3, allocation_4]
        Session.add_all(allocations)
        Session.flush()
        assert_equal(
            list(allocation_summary(allocations)),
            [(allocation_1, 0, 0, 0),
             (allocation_2, 0, 0, 20),
             (allocation_3, 0, 0, 30),
             (allocation_4, 0, 0, 0)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_holds (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        Hold(allocation_1, 10)
        h2 = Hold(allocation_2, 15)
        Hold(allocation_2, 5)
        Hold(allocation_4, 9)
        h5 = Hold(allocation_4, 8)
        h2.active = False
        h5.active = False
        allocations = [allocation_1, allocation_2, allocation_3, allocation_4]
        Session.add_all(allocations)
        Session.flush()
        assert_equal(
            list(allocation_summary(allocations)),
            [(allocation_1, 0, 0, 0),
             (allocation_2, 0, 0, 15),
             (allocation_3, 0, 0, 30),
             (allocation_4, 0, 0, 26)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_charges (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        Charge(allocation_1, 10)
        Charge(allocation_2, 15)
        Charge(allocation_2, 5)
        Charge(allocation_4, 9)
        Charge(allocation_4, 8)
        allocations = [allocation_1, allocation_2, allocation_3, allocation_4]
        Session.add_all(allocations)
        Session.flush()
        assert_equal(
            list(allocation_summary(allocations)),
            [(allocation_1, 0, 10, 0),
             (allocation_2, 0, 20, 0),
             (allocation_3, 0, 0, 30),
             (allocation_4, 0, 17, 18)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_expired_charges (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, start)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, start)
        Charge(allocation_1, 10)
        Charge(allocation_2, 15)
        Charge(allocation_2, 5)
        Charge(allocation_4, 9)
        Charge(allocation_4, 8)
        allocations = [allocation_1, allocation_2, allocation_3, allocation_4]
        Session.add_all(allocations)
        Session.flush()
        assert_equal(
            list(allocation_summary(allocations)),
            [(allocation_1, 0, 10, 0),
             (allocation_2, 0, 20, 0),
             (allocation_3, 0, 0, 30),
             (allocation_4, 0, 17, 0)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_refunds (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        charge_1 = Charge(allocation_1, 10)
        charge_2 = Charge(allocation_2, 15)
        Charge(allocation_2, 5)
        Charge(allocation_4, 9)
        charge_5 = Charge(allocation_4, 8)
        Refund(charge_1, 4)
        Refund(charge_2, 3)
        Refund(charge_2, 5)
        Refund(charge_5, 8)
        allocations = [allocation_1, allocation_2, allocation_3, allocation_4]
        Session.add_all(allocations)
        Session.flush()
        assert_equal(
            list(allocation_summary(allocations)),
            [(allocation_1, 0, 6, 4),
             (allocation_2, 0, 12, 8),
             (allocation_3, 0, 0, 30),
             (allocation_4, 0, 9, 26)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_users_filter (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        user_1 = User.cached("1")
        user_2 = User.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        charge_1 = Charge(allocation_1, 10)
        charge_2 = Charge(allocation_2, 15)
        charge_3 = Charge(allocation_2, 5)
        charge_4 = Charge(allocation_4, 9)
        charge_5 = Charge(allocation_4, 8)
        Refund(charge_1, 4)
        Refund(charge_2, 3)
        Refund(charge_2, 5)
        Refund(charge_5, 8)
        Hold(allocation_4, 9)
        hold_2 = Hold(allocation_4, 8)
        hold_2.active = False
        charge_1.job = Job("1.1")
        charge_2.job = Job("1.2")
        charge_3.job = Job("1.3")
        charge_4.job = Job("2.1")
        charge_5.job = Job("2.2")
        charge_1.job.user = user_1
        charge_2.job.user = user_2
        charge_3.job.user = user_1
        charge_4.job.user = user_1
        charge_5.job.user = user_2
        allocations = [allocation_1, allocation_2, allocation_3, allocation_4]
        Session.add_all(allocations)
        Session.flush()
        assert_equal(
            list(allocation_summary(allocations, users=[user_1])),
            [(allocation_1, 1, 6, 4),
             (allocation_2, 1, 5, 8),
             (allocation_3, 0, 0, 30),
             (allocation_4, 1, 9, 17)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_after_filter (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        user_1 = User.cached("1")
        user_2 = User.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        charge_1 = Charge(allocation_1, 10)
        charge_2 = Charge(allocation_2, 15)
        charge_3 = Charge(allocation_2, 5)
        charge_4 = Charge(allocation_4, 9)
        charge_5 = Charge(allocation_4, 8)
        Refund(charge_1, 4)
        Refund(charge_2, 3)
        Refund(charge_2, 5)
        Refund(charge_5, 8)
        Hold(allocation_4, 9)
        hold_2 = Hold(allocation_4, 8)
        hold_2.active = False
        charge_1.job = Job("1.1")
        charge_2.job = Job("1.2")
        charge_3.job = Job("1.3")
        charge_4.job = Job("2.1")
        charge_5.job = Job("2.2")
        charge_1.job.user = user_1
        charge_2.job.user = user_2
        charge_3.job.user = user_1
        charge_4.job.user = user_1
        charge_5.job.user = user_2
        charge_1.datetime = datetime(2000, 1, 2)
        charge_2.datetime = datetime(2000, 1, 3)
        charge_3.datetime = datetime(2000, 1, 4)
        charge_4.datetime = datetime(2000, 1, 5)
        charge_5.datetime = datetime(2000, 1, 6)
        charge_1.job.end = datetime(2000, 1, 2)
        charge_2.job.end = datetime(2000, 1, 3)
        charge_3.job.end = datetime(2000, 1, 4)
        charge_4.job.end = datetime(2000, 1, 5)
        charge_5.job.end = datetime(2000, 1, 6)
        allocations = [allocation_1, allocation_2, allocation_3, allocation_4]
        Session.add_all(allocations)
        Session.flush()
        assert_equal(
            list(allocation_summary(allocations, after=datetime(2000, 1, 4))),
            [(allocation_1, 0, 0, 4),
             (allocation_2, 0, 5, 8),
             (allocation_3, 0, 0, 30),
             (allocation_4, 2, 9, 17)])

    @patch("cbank.model.queries.datetime", datetime_mock)
    def test_before_filter (self):
        project_1 = Project.cached("1")
        project_2 = Project.cached("2")
        resource_1 = Resource.cached("1")
        resource_2 = Resource.cached("2")
        user_1 = User.cached("1")
        user_2 = User.cached("2")
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        allocation_1 = Allocation(project_1, resource_1, 10, start, end)
        allocation_2 = Allocation(project_1, resource_1, 20, start, end)
        allocation_3 = Allocation(project_2, resource_1, 30, start, end)
        allocation_4 = Allocation(project_2, resource_2, 35, start, end)
        charge_1 = Charge(allocation_1, 10)
        charge_2 = Charge(allocation_2, 15)
        charge_3 = Charge(allocation_2, 5)
        charge_4 = Charge(allocation_4, 9)
        charge_5 = Charge(allocation_4, 8)
        Refund(charge_1, 4)
        Refund(charge_2, 3)
        Refund(charge_2, 5)
        Refund(charge_5, 8)
        Hold(allocation_4, 9)
        hold_2 = Hold(allocation_4, 8)
        hold_2.active = False
        charge_1.job = Job("1.1")
        charge_2.job = Job("1.2")
        charge_3.job = Job("1.3")
        charge_4.job = Job("2.1")
        charge_5.job = Job("2.2")
        charge_1.job.user = user_1
        charge_2.job.user = user_2
        charge_3.job.user = user_1
        charge_4.job.user = user_1
        charge_5.job.user = user_2
        charge_1.datetime = datetime(2000, 1, 2)
        charge_2.datetime = datetime(2000, 1, 3)
        charge_3.datetime = datetime(2000, 1, 4)
        charge_4.datetime = datetime(2000, 1, 5)
        charge_5.datetime = datetime(2000, 1, 6)
        charge_1.job.start = datetime(2000, 1, 1)
        charge_2.job.start = datetime(2000, 1, 2)
        charge_3.job.start = datetime(2000, 1, 3)
        charge_4.job.start = datetime(2000, 1, 4)
        charge_5.job.start = datetime(2000, 1, 5)
        allocations = [allocation_1, allocation_2, allocation_3, allocation_4]
        Session.add_all(allocations)
        Session.flush()
        assert_equal(
            list(allocation_summary(allocations, before=datetime(2000, 1, 4))),
            [(allocation_1, 1, 6, 4),
             (allocation_2, 2, 7, 8),
             (allocation_3, 0, 0, 30),
             (allocation_4, 0, 0, 17)])
