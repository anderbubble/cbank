from testsuite import (
    BaseTester, assert_identical, assert_not_identical,
    clear_mappers, restore_mappers, assert_in)

from nose.tools import (
    raises, assert_equal, assert_not_equal, assert_almost_equals)

from mock import Mock, patch, sentinel

from datetime import datetime, timedelta

from sqlalchemy import create_engine

from cbank import config
from cbank.model.entities import (
    UpstreamEntity, User, Project, Resource,
    Allocation, Hold, Charge, Refund)
from cbank.model.queries import Session
import cbank.model


def setup ():
    clear_mappers()


def teardown ():
    restore_mappers()


class TestUpstreamEntity (BaseTester):

    def test_init (self):
        entity = UpstreamEntity(sentinel.entity_id)
        assert_equal(entity.id, sentinel.entity_id)

    @patch.object(UpstreamEntity, "_out",
                  Mock([], return_value=None))
    def test_str_undefined (self, *args):
        assert_equal(str(UpstreamEntity("1")), "1")
        UpstreamEntity._out.assert_called_with("1")

    @patch.object(UpstreamEntity, "_out",
                  Mock([], return_value="one"))
    def test_str_defined (self):
        assert_equal(str(UpstreamEntity("1")), "one")
        UpstreamEntity._out.assert_called_with("1")

    def test_fetch_identity (self):
        assert_identical(
            UpstreamEntity.fetch("one"),
            UpstreamEntity.fetch("one"))
        assert_not_identical(
            UpstreamEntity.fetch("one"),
            UpstreamEntity.fetch("two"))

    @patch.object(UpstreamEntity, "_in",
                  Mock([], return_value=None))
    def test_fetch_undefined (self):
        entity = UpstreamEntity.fetch("one")
        assert_equal(entity.id, "one")
        UpstreamEntity._in.assert_called_with("one")

    @patch.object(UpstreamEntity, "_in",
                  Mock([], return_value="1"))
    def test_fetch_defined (self):
        entity = UpstreamEntity.fetch("one")
        assert_equal(entity.id, "1")
        UpstreamEntity._in.assert_called_with("one")

    def test_cached (self):
        assert_identical(
            UpstreamEntity.cached("1"),
            UpstreamEntity.cached("1"))
        assert_not_identical(
            UpstreamEntity.cached("1"),
            UpstreamEntity.cached("2"))

    def test_eq (self):
        assert_equal(
            UpstreamEntity("1"),
            UpstreamEntity("1"))
        assert_not_equal(
            UpstreamEntity("1"),
            UpstreamEntity("2"))


class TestUser (BaseTester):

    @patch.object(User, "_member",
                  Mock([], return_value=False))
    def test_not_member (self):
        project = Mock(['id'])
        project.id = sentinel.project_id
        assert not User(sentinel.user_id).is_member(project)
        User._member.assert_called_with(
            sentinel.project_id, sentinel.user_id)

    @patch.object(User, "_member",
                  Mock([], return_value=True))
    def test_member (self):
        project = Mock(['id'])
        project.id = sentinel.project_id
        assert User(sentinel.user_id).is_member(project)
        User._member.assert_called_with(
            sentinel.project_id, sentinel.user_id)

    @patch.object(User, "_manager",
                  Mock([], return_value=False))
    def test_not_manager (self):
        project = Mock(['id'])
        project.id = sentinel.project_id
        assert not User(sentinel.user_id).is_manager(project)
        User._manager.assert_called_with(
            sentinel.project_id, sentinel.user_id)

    @patch.object(User, "_manager",
                  Mock([], return_value=True))
    def test_manager (self):
        project = Mock(['id'])
        project.id = sentinel.project_id
        assert User(sentinel.user_id).is_manager(project)
        User._manager.assert_called_with(
            sentinel.project_id, sentinel.user_id)


class TestAllocation (BaseTester):

    datetime_mock = Mock(['now'])
    datetime_mock.now = Mock([], return_value=sentinel.now)

    @patch("cbank.model.entities.datetime", datetime_mock)
    def test_init (self):
        project = Mock(['id'])
        project.id = sentinel.project_id
        resource = Mock(['id'])
        resource.id = sentinel.resource_id
        allocation = Allocation(
            project, resource, sentinel.amount,
            sentinel.start, sentinel.end)
        assert_equal(allocation.id, None)
        assert_equal(allocation.datetime, sentinel.now)
        assert_equal(allocation.project_id, sentinel.project_id)
        assert_equal(allocation.resource_id, sentinel.resource_id)
        assert_equal(allocation.amount, sentinel.amount)
        assert_equal(allocation.comment, None)
        assert_equal(allocation.start, sentinel.start)
        assert_equal(allocation.end, sentinel.end)
        assert_equal(allocation.holds, [])
        assert_equal(allocation.charges, [])

    @patch.object(Project, "cached",
                  Mock([], return_value=sentinel.project))
    def test_project (self):
        allocation = Allocation(
            None, None, None, None, None)
        assert_equal(allocation.project, None)
        project = Mock(['id'])
        project.id = sentinel.project_id
        allocation.project = project
        assert_equal(allocation.project, sentinel.project)
        Project.cached.assert_called_with(sentinel.project_id)

    @patch.object(Resource, "cached",
                  Mock([], return_value=sentinel.resource))
    def test_resource (self):
        allocation = Allocation(
            None, None, None, None, None)
        assert_equal(allocation.resource, None)
        resource = Mock(['id'])
        resource.id = sentinel.resource_id
        allocation.resource = resource
        assert_equal(allocation.resource, sentinel.resource)
        Resource.cached.assert_called_with(sentinel.resource_id)

    @patch.object(Allocation.active.im_func, "func_defaults",
                  (datetime(2000, 1, 2), ))
    def test_active (self):
        start = datetime(2000, 1, 1)
        end = datetime(2000, 1, 3)
        allocation = Allocation(None, None, None, start, end)
        assert allocation.active()
        assert not allocation.active(start-timedelta(hours=1))
        assert allocation.active(start)
        assert not allocation.active(end)
        assert not allocation.active(end+timedelta(hours=1))

    def test_amount_held (self):
        allocation = Allocation(None, None, None, None, None)
        assert_equal(allocation.amount_held(), 0)
        hold_1 = Mock(['amount', 'active'])
        hold_1.amount = 1
        hold_1.active = True
        allocation.holds.append(hold_1)
        assert_equal(allocation.amount_held(), 1)
        hold_2 = Mock(['amount', 'active'])
        hold_2.amount = 2
        hold_2.active = True
        allocation.holds.append(hold_2)
        assert_equal(allocation.amount_held(), 3)
        hold_1.active = False
        assert_equal(allocation.amount_held(), 2)
        hold_2.active = False
        assert_equal(allocation.amount_held(), 0)

    def test_amount_charged (self):
        allocation = Allocation(None, None, None, None, None)
        assert_equal(allocation.amount_charged(), 0)
        charge_1 = Mock(['effective_amount'])
        charge_1.effective_amount = Mock(return_value=1)
        allocation.charges.append(charge_1)
        assert_equal(allocation.amount_charged(), 1)
        charge_2 = Mock(['effective_amount'])
        charge_2.effective_amount = Mock(return_value=2)
        allocation.charges.append(charge_2)
        assert_equal(allocation.amount_charged(), 3)

    def test_amount_available (self):
        allocation = Allocation(None, None, 0, None, None)
        assert_equal(allocation.amount_available(), 0)
        allocation.amount = 10
        assert_equal(allocation.amount_available(), 10)
        hold_1 = Mock(['amount', 'active'])
        hold_1.amount = 1
        hold_1.active = True
        allocation.holds.append(hold_1)
        assert_equal(allocation.amount_available(), 9)
        hold_2 = Mock(['amount', 'active'])
        hold_2.amount = 2
        hold_2.active = True
        allocation.holds.append(hold_2)
        assert_equal(allocation.amount_available(), 7)
        hold_1.active = False
        assert_equal(allocation.amount_available(), 8)
        hold_2.active = False
        assert_equal(allocation.amount_available(), 10)
        charge_1 = Mock(['effective_amount'])
        charge_1.effective_amount = Mock(return_value=1)
        allocation.charges.append(charge_1)
        assert_equal(allocation.amount_available(), 9)
        charge_2 = Mock(['effective_amount'])
        charge_2.effective_amount = Mock(return_value=2)
        allocation.charges.append(charge_2)
        assert_equal(allocation.amount_available(), 7)
        charge_3 = Mock(['effective_amount'])
        charge_3.effective_amount = Mock(return_value=8)
        allocation.charges.append(charge_3)
        assert_equal(allocation.amount_available(), 0)


class TestHold (BaseTester):

    datetime_mock = Mock(['now'])
    datetime_mock.now = Mock([], return_value=sentinel.now)

    @patch("cbank.model.entities.datetime", datetime_mock)
    def test_init (self):
        hold = Hold(sentinel.allocation, sentinel.amount)
        assert_equal(hold.id, None)
        assert_equal(hold.datetime, sentinel.now)
        assert_equal(hold.allocation, sentinel.allocation)
        assert_equal(hold.amount, sentinel.amount)
        assert_equal(hold.comment, None)
        assert hold.active

    @raises(ValueError)
    def test_distributed_without_allocations (self):
        holds = Hold.distributed([], amount=1)

    def test_distributed (self):
        allocation_1 = Mock(['amount_available'])
        allocation_1.amount_available = Mock([], return_value=600)
        allocation_2 = Mock(['amount_available'])
        allocation_2.amount_available = Mock([], return_value=600)
        allocations = [allocation_1, allocation_2]
        holds = Hold.distributed(allocations, amount=900)
        assert_equal(len(holds), 2)
        assert_equal(holds[0].allocation, allocation_1)
        assert_equal(holds[0].amount, 600)
        assert_equal(holds[1].allocation, allocation_2)
        assert_equal(holds[1].amount, 300)

    def test_distributed_zero_amount (self):
        holds = Hold.distributed([Mock([])], amount=0)

    @raises(ValueError)
    def test_distributed_greater_than_allocation (self):
        allocation = Mock(['amount_available'])
        allocation.amount_available = Mock([], return_value=1)
        holds = Hold.distributed([allocation], amount=2)


class TestCharge (BaseTester):

    datetime_mock = Mock(['now'])
    datetime_mock.now = Mock([], return_value=sentinel.now)

    @patch("cbank.model.entities.datetime", datetime_mock)
    def test_init (self):
        charge = Charge(sentinel.allocation, sentinel.amount)
        assert_equal(charge.id, None)
        assert_equal(charge.datetime, sentinel.now)
        assert_equal(charge.allocation, sentinel.allocation)
        assert_equal(charge.amount, sentinel.amount)
        assert_equal(charge.comment, None)
        assert_equal(charge.refunds, [])

    @raises(ValueError)
    def test_distributed_without_allocations (self):
        charges = Charge.distributed([], amount=1)

    def test_distributed (self):
        allocation_1 = Mock(['amount_available'])
        allocation_1.amount_available = Mock([], return_value=600)
        allocation_2 = Mock(['amount_available'])
        allocation_2.amount_available = Mock([], return_value=600)
        allocations = [allocation_1, allocation_2]
        charges = Charge.distributed(allocations, amount=900)
        assert_equal(len(charges), 2)
        assert_equal(charges[0].allocation, allocation_1)
        assert_equal(charges[0].amount, 600)
        assert_equal(charges[1].allocation, allocation_2)
        assert_equal(charges[1].amount, 300)

    def test_distributed_zero_amount (self):
        charges = Charge.distributed([Mock([])], amount=0)

    def test_distributed_greater_than_allocation (self):
        allocation = Mock(['amount_available'])
        allocation.amount_available = Mock([], return_value=1)
        charges = Charge.distributed([allocation], amount=2)
        assert_equal(len(charges), 1)
        assert_equal(charges[0].amount, 2)

    def test_amount_refunded (self):
        charge = Charge(None, None)
        assert_equal(charge.amount_refunded(), 0)
        refund_1 = Mock(['amount'])
        refund_1.amount = 1
        charge.refunds.append(refund_1)
        assert_equal(charge.amount_refunded(), 1)
        refund_2 = Mock(['amount'])
        refund_2.amount = 2
        charge.refunds.append(refund_2)
        assert_equal(charge.amount_refunded(), 3)

    def test_effective_amount (self):
        charge = Charge(None, 10)
        assert_equal(charge.effective_amount(), 10)
        refund_1 = Mock(['amount'])
        refund_1.amount = 1
        charge.refunds.append(refund_1)
        assert_equal(charge.effective_amount(), 9)
        refund_2 = Mock(['amount'])
        refund_2.amount = 2
        charge.refunds.append(refund_2)
        assert_equal(charge.effective_amount(), 7)

    @patch("cbank.model.entities.Refund",
           Mock(return_value=sentinel.refund))
    def test_refund (self):
        charge = Charge(None, None)
        refund = charge.refund(sentinel.refund_amount)
        cbank.model.entities.Refund.assert_called_with(
            charge, sentinel.refund_amount)
        assert_equal(refund, sentinel.refund)

    @patch("cbank.model.entities.Refund", Mock([]))
    def test_default_refund (self):
        charge = Charge(None, 10)
        charge.refunds.append(Refund(charge, 3))
        charge.refund()
        cbank.model.entities.Refund.assert_called_with(
            charge, 7)


class TestRefund (BaseTester):
    
    datetime_mock = Mock(['now'])
    datetime_mock.now = Mock([], return_value=sentinel.now)

    @patch("cbank.model.entities.datetime", datetime_mock)
    def test_init (self):
        refund = Refund(sentinel.charge, sentinel.refund_amount)
        assert_equal(refund.id, None)
        assert_equal(refund.datetime, sentinel.now)
        assert_equal(refund.charge, sentinel.charge)
        assert_equal(refund.amount, sentinel.refund_amount)
        assert_equal(refund.comment, None)
