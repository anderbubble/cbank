from nose.tools import raises, assert_equal

from datetime import datetime

from sqlalchemy import create_engine

import clusterbank.model
import clusterbank.upstreams.default
from clusterbank.model import (
    User, Project, Resource, Allocation, Hold, Job, Charge, Refund)
from clusterbank.controllers import Session, get_projects


def setup ():
    clusterbank.model.metadata.bind = create_engine("sqlite:///:memory:")
    clusterbank.model.use_upstream(clusterbank.upstreams.default)


def teardown ():
    clusterbank.model.clear_upstream()
    clusterbank.model.metadata.bind = None


def assert_identical (obj1, obj2):
    assert obj1 is obj2, "%r is not %r" % (obj1, obj2)


class DatabaseEnabledTester (object):

    def setup (self):
        """Create the tables before each test."""
        clusterbank.model.metadata.create_all()
    
    def teardown (self):
        """drop the database after each test."""
        Session.remove()
        clusterbank.model.metadata.drop_all()


class TestGetProjects (DatabaseEnabledTester):

    def setup (self):
        DatabaseEnabledTester.setup(self)
        p1 = clusterbank.upstreams.default.Project("1", "one")
        p2 = clusterbank.upstreams.default.Project("2", "two")
        u1 = clusterbank.upstreams.default.Project("1", "one")
        u2 = clusterbank.upstreams.default.Project("2", "two")
        p1.members = [u1]
        p1.managers = [u2]
        p2.members = [u2]
        p2.managers = [u1]
        clusterbank.upstreams.default.projects = [p1, p2]
        clusterbank.upstreams.default.users = [u1, u2]
        a1 = Allocation(Project.cached("1"), Resource.cached("1"), 1,
                       datetime.now(), datetime.now())
        a2 = Allocation(Project.cached("2"), Resource.cached("1"), 1,
                       datetime.now(), datetime.now())
        Session.add_all([a1, a2])
        Session.commit()

    def teardown (self):
        DatabaseEnabledTester.teardown(self)
        clusterbank.upstreams.default.projects = []
        clusterbank.upstreams.default.users = []

    def test_all_projects (self):
        assert_equal(set(get_projects()),
                     set([Project.cached("1"), Project.cached("2")]))

    def test_member_projects (self):
        assert_equal(get_projects(member=User.cached("1")), [Project.cached("1")])

    def test_manager_projects (self):
        assert_equal(get_projects(manager=User.cached("1")), [Project.cached("2")])


class TestPositiveAmountConstraints (DatabaseEnabledTester):

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


class TestHoldConstraints (DatabaseEnabledTester):

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


class TestRefundConstraints (DatabaseEnabledTester):

    @raises(ValueError)
    def test_refund_gt (self):
        a = Allocation(Project("1"), Resource("1"), 10,
                       datetime.now(), datetime.now())
        c = Charge(a, 5)
        r = Refund(c, 6)
        Session.add(r)
        Session.commit()
