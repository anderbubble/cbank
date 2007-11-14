import os
from datetime import datetime, timedelta

import clusterbank.model
from clusterbank.interfaces.cbank import main, parse_directive

def run (command):
    argv = command.split()
    return main(argv)


class ScriptTester (object):
    
    def setup (self):
        clusterbank.model.metadata.create_all()
    
    def teardown (self):
        clusterbank.model.Session.remove()
        clusterbank.model.metadata.drop_all()


def test_parse_directive ():
    assert parse_directive("req") == "request"
    assert parse_directive("allo") == "allocation"
    assert parse_directive("ref") == "refund"
    assert parse_directive("re") == "re"


class TestMain (ScriptTester):
    
    def test_request (self):
        requests = run("cbank request --project grail --resource spam --amount 1000 --start 2007-01-01 --comment testing")
        assert len(requests) == 1
        request = requests[0]
        assert request.id is not None
        assert datetime.now() - request.datetime < timedelta(minutes=1)
        assert request.project.name == "grail"
        assert request.resource.name == "spam"
        assert request.amount == 1000
        assert request.start == datetime(year=2007, month=1, day=1)
        assert request.comment == "testing"
    
    def test_request_list (self):
        requests = run("cbank request --list")
        assert not list(requests)
        new_requests = run("cbank request --project grail --resource spam --amount 1000")
        list_requests = run("cbank request --list --project grail --resource spam")
        assert len(list(list_requests)) == len(new_requests)
        assert set(list_requests) == set(new_requests)
    
    def test_allocation (self):
        requests = run("cbank request --project grail --resource spam --amount 1000 --start 2007-01-01 --comment testing")
        allocations = run("cbank allocation --amount 700 --project grail --resource spam --request %i --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        assert len(allocations) == 1
        allocation = allocations[0]
        assert allocation.id is not None
        assert datetime.now() - allocation.datetime < timedelta(minutes=1)
        assert allocation.project.name == "grail"
        assert allocation.resource.name == "spam"
        assert allocation.start == datetime(year=2007, month=1, day=1)
        assert allocation.expiration == datetime(year=2008, month=1, day=1)
    
    def test_allocation_list (self):
        allocations = run("cbank allocation --list")
        assert not list(allocations)
        new_allocations = run("cbank allocation --project grail --resource spam --amount 700 --start 2007-01-01 --expiration 2008-01-01")
        list_allocations = run("cbank allocation --list --project grail --resource spam")
        assert len(list(list_allocations)) == len(new_allocations)
        assert set(list_allocations) == set(new_allocations)
    
    def test_hold (self):
        allocations = run("cbank allocation --project grail --resource spam --amount 700 --start 2007-01-01 --expiration 2008-01-01")
        holds = run("cbank hold --allocation %i --amount 100 --comment testing" % allocations[0].id)
        assert len(holds) == 1
        hold = holds[0]
        assert hold.id is not None
        assert datetime.now() - hold.datetime < timedelta(minutes=1)
        assert hold.allocation is allocations[0]
        assert hold.amount == 100
        assert hold.comment == "testing"
    
    def test_hold_distributed (self):
        allocations = run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01")
        allocations.extend(run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01"))
        holds = run("cbank hold --project grail --resource spam --amount 150 --comment testing")
        assert len(holds) == 2
        assert sum(hold.amount for hold in holds) == 150
        for hold in holds:
            assert hold.allocation in allocations
    
    def test_hold_list (self):
        holds = run("cbank hold --list")
        assert not list(holds)
        allocations = run("cbank allocation --project grail --resource spam --amount 700 --start 2007-01-01 --expiration 2008-01-01")
        new_holds = run("cbank hold --allocation %i --amount 100" % allocations[0].id)
        list_holds = run("cbank hold --list --project grail --resource spam --allocation %i" % allocations[0].id)
        assert len(list(list_holds)) == len(new_holds)
        assert set(list_holds) == set(new_holds)
    
    def test_charge (self):
        allocations = run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01")
        charges = run("cbank charge --allocation %i --amount 50" % allocations[0].id)
        assert len(charges) == 1
        charge = charges[0]
        assert charge.id is not None
        assert datetime.now() - charge.datetime < timedelta(minutes=1)
        assert charge.amount == 50
        assert charge.allocation is allocations[0]
    
    def test_charge_distributed (self):
        allocations = run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01")
        allocations.extend(run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01"))
        charges = run("cbank charge --project grail --resource spam --amount 150 --comment testing")
        assert len(charges) == 2
        assert sum(charge.amount for charge in charges)
        for charge in charges:
            assert charge.allocation in allocations
            assert charge.comment == "testing"
    
    def test_charge_list (self):
        charges = run("cbank charge --list")
        assert not list(charges)
        allocations = run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01")
        new_charges = run("cbank charge --allocation %i --amount 50" % allocations[0].id)
        list_charges = run("cbank charge --list --project grail --resource spam --allocation %i" % allocations[0].id)
        assert len(list(list_charges)) == len(new_charges)
        assert set(list_charges) == set(new_charges)
    
    def test_refund (self):
        allocations = run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01")
        charges = run("cbank charge --allocation %i --amount 50" % allocations[0].id)
        refunds = run("cbank refund --charge %i --amount 25 --comment testing" % charges[0].id)
        assert len(refunds) == 1
        refund = refunds[0]
        assert refund.id is not None
        assert refund.charge is charges[0]
        assert refund.amount == 25
        assert refund.comment == "testing"
    
    def test_refund_list (self):
        refunds = run("cbank refund --list")
        assert not list(refunds)
        allocations = run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01")
        charges = run("cbank charge --allocation %i --amount 50" % allocations[0].id)
        new_refunds = run("cbank refund --charge %i --amount 25" % charges[0].id)
        list_refunds = run("cbank refund --list --project grail --resource spam --charge %i --allocation %i" % (charges[0].id, allocations[0].id))
        assert len(list(list_refunds)) == len(new_refunds)
        assert set(list_refunds) == set(new_refunds)
