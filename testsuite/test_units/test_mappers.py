from nose.tools import raises, assert_equal

from mock import Mock, patch

from testsuite import BaseTester

from datetime import datetime, timedelta

from cbank.model.entities import (
    User, Project, Resource, Allocation, Hold, Job, Charge, Refund)
from cbank.model.queries import Session


class MapperTester (BaseTester):

    def setup (self):
        self.setup_database()
    
    def teardown (self):
        Session.remove()
        self.teardown_database()


class TestAllocationMapper (MapperTester):

    def test_hold_sum_zero (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        Session.add(allocation)
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation._active_hold_sum, 0)

    def test_hold_sum_one (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        hold = Hold(allocation, 1)
        Session.add_all([allocation, hold])
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation._active_hold_sum, 1)

    def test_hold_sum_two (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        hold_1 = Hold(allocation, 1)
        hold_2 = Hold(allocation, 2)
        Session.add_all([allocation, hold_1, hold_2])
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation._active_hold_sum, 3)

    def test_hold_sum_two_with_one_inactive (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        hold_1 = Hold(allocation, 1)
        hold_1.active = False
        hold_2 = Hold(allocation, 2)
        Session.add_all([allocation, hold_1, hold_2])
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation._active_hold_sum, 2)

    def test_charge_sum_zero (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        Session.add(allocation)
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation._charge_sum, 0)

    def test_charge_sum_one (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        charge = Charge(allocation, 1)
        Session.add_all([allocation, charge])
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation._charge_sum, 1)

    def test_charge_sum_two (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        charge_1 = Charge(allocation, 1)
        charge_2 = Charge(allocation, 2)
        Session.add_all([allocation, charge_1, charge_2])
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation._charge_sum, 3)

    def test_refund_sum_zero (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        Session.add(allocation)
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation._refund_sum, 0)

    def test_refund_sum_one (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        charge = Charge(allocation, 0)
        refund = Refund(charge, 1)
        Session.add_all([allocation, charge, refund])
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation._refund_sum, 1)

    def test_refund_sum_two (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        charge = Charge(allocation, 0)
        refund_1 = Refund(charge, 1)
        refund_2 = Refund(charge, 2)
        Session.add_all([allocation, charge, refund_1, refund_2])
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation._refund_sum, 3)

    def test_refund_sum_two_on_different_charges (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        charge_1 = Charge(allocation, 0)
        charge_2 = Charge(allocation, 0)
        refund_1 = Refund(charge_1, 1)
        refund_2 = Refund(charge_2, 2)
        Session.add_all([allocation, charge_1, charge_2, refund_1, refund_2])
        Session.commit()
        Session.close()
        allocation = Session.query(Allocation).one()
        assert_equal(allocation._refund_sum, 3)


class TestChargeMapper (MapperTester):

    def test_refund_sum_zero (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        charge = Charge(allocation, 0)
        Session.add_all([allocation, charge])
        Session.commit()
        Session.close()
        charge = Session.query(Charge).one()
        assert_equal(charge._refund_sum, 0)

    def test_refund_sum_one (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        charge = Charge(allocation, 0)
        refund = Refund(charge, 1)
        Session.add_all([allocation, charge, refund])
        Session.commit()
        Session.close()
        charge = Session.query(Charge).one()
        assert_equal(charge._refund_sum, 1)

    def test_refund_sum_two (self):
        allocation = Allocation(None, None, 0, datetime(2000, 1, 1), datetime(2001, 1, 1))
        allocation.project_id = "project"
        allocation.resource_id = "resource"
        charge = Charge(allocation, 0)
        refund_1 = Refund(charge, 1)
        refund_2 = Refund(charge, 2)
        Session.add_all([allocation, charge, refund_1, refund_2])
        Session.commit()
        Session.close()
        charge = Session.query(Charge).one()
        assert_equal(charge._refund_sum, 3)
