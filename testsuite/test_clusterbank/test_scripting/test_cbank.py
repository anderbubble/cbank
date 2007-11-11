import os
from datetime import datetime

import clusterbank.model
from clusterbank.scripting import cbank

def run (command):
    argv = command.split()
    return cbank.main(argv)


class ScriptTester (object):
    
    def setup (self):
        clusterbank.model.metadata.create_all()
    
    def teardown (self):
        clusterbank.model.Session.remove()
        clusterbank.model.metadata.drop_all()


class TestMain (ScriptTester):
    
    def test_request (self):
        requests = run("cbank request --project grail --resource spam --amount 1000 --start 2007-01-01 --comment testing")
        assert len(requests) == 1
        request = requests[0]
        assert request.id is not None
        assert request.project.name == "grail"
        assert request.resource.name == "spam"
        assert request.amount == 1000
        assert request.start == datetime(year=2007, month=1, day=1)
        assert request.comment == "testing"
    
    def test_request_list (self):
        requests = run("cbank request --list")
        assert not requests
        new_requests = run("cbank request --project grail --resource spam --amount 1000")
        list_requests = run("cbank request --list --project grail --resource spam")
        assert len(list_requests) == len(new_requests)
        assert set(list_requests) == set(new_requests)
    
    def test_allocation (self):
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        assert len(allocations) == 1
        allocation = allocations[0]
        assert allocation.id is not None
        assert allocation.request is requests[0]
        assert allocation.start == datetime(year=2007, month=1, day=1)
        assert allocation.expiration == datetime(year=2008, month=1, day=1)
    
    def test_allocation_list (self):
        allocations = run("cbank allocation --list")
        assert not allocations
        requests = run("cbank request --project grail --resource spam --amount 1000")
        new_allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        list_allocations = run("cbank allocation --list --project grail --resource spam --request %s" % requests[0].id)
        assert len(list_allocations) == len(new_allocations)
        assert set(list_allocations) == set(new_allocations)
    
    def test_hold (self):
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        holds = run("cbank hold --allocation %s --amount 100 --comment testing" % allocations[0].id)
        assert len(holds) == 1
        hold = holds[0]
        assert hold.id is not None
        assert hold.allocation is allocations[0]
        assert hold.amount == 100
        assert hold.comment == "testing"
    
    def test_hold_distributed (self):
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        holds = run("cbank hold --project grail --resource spam --amount 100 --comment testing")
        assert len(holds) == 1
        for hold in holds:
            assert hold.id is not None
            assert hold.allocation in allocations
            assert hold.amount == 100
            assert hold.comment == "testing"
    
    def test_hold_negative_amount (self):
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        try:
            run("cbank hold --allocation %s --amount -100 --comment testing" % allocations[0].id)
        except ValueError:
            pass
        else:
            assert not "allowed negative hold"
    
    def test_hold_list (self):
        holds = run("cbank hold --list")
        assert not holds
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        new_holds = run("cbank hold --allocation %s --amount 100" % allocations[0].id)
        list_holds = run("cbank hold --list --project grail --resource spam --allocation %s --request %s" % (allocations[0].id, requests[0].id))
        assert len(list_holds) == len(new_holds)
        assert set(list_holds) == set(new_holds)
    
    def test_charge (self):
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        holds = run("cbank hold --allocation %s --amount 100" % allocations[0].id)
        charges = run("cbank charge --hold %s --amount 50" % holds[0].id)
        assert len(charges) == 1
        charge = charges[0]
        assert charge.id is not None
        assert charge.amount == 50
        assert charge.hold is holds[0]
    
    def test_charge_distributed (self):
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        holds = run("cbank hold --allocation %s --amount 100 --comment testing" % allocations[0].id)
        charges = run("cbank charge --project grail --resource spam --amount 50 --comment testing")
        assert len(charges) == 1
        for charge in charges:
            assert charge.id is not None
            assert charge.hold in holds
            assert charge.amount == 50
            assert charge.comment == "testing"
    
    def test_charge_negative_amount (self):
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        holds = run("cbank hold --allocation %s --amount 100" % allocations[0].id)
        try:
            run("cbank charge --hold %s --amount -50" % holds[0].id)
        except ValueError:
            pass
        else:
            assert False
    
    def test_charge_list (self):
        charges = run("cbank charge --list")
        assert not charges
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        holds = run("cbank hold --allocation %s --amount 100" % allocations[0].id)
        new_charges = run("cbank charge --hold %s --amount 50" % holds[0].id)
        list_charges = run("cbank charge --list --project grail --resource spam --hold %s --allocation %s --request %s" % (holds[0].id, allocations[0].id, requests[0].id))
        assert len(list_charges) == len(new_charges)
        assert set(list_charges) == set(new_charges)
    
    def test_refund (self):
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        holds = run("cbank hold --allocation %s --amount 100" % allocations[0].id)
        charges = run("cbank charge --hold %s --amount 50" % holds[0].id)
        refunds = run("cbank refund --charge %s --amount 25 --comment testing" % charges[0].id)
        assert len(refunds) == 1
        refund = refunds[0]
        assert refund.id is not None
        assert refund.charge is charges[0]
        assert refund.amount == 25
        assert refund.comment == "testing"
    
    def test_refund_negative_amount (self):
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        holds = run("cbank hold --allocation %s --amount 100" % allocations[0].id)
        charges = run("cbank charge --hold %s --amount 50" % holds[0].id)
        try:
            run("cbank refund --charge %s --amount -25" % charges[0].id)
        except ValueError:
            pass
        else:
            assert False
    
    def test_refund_list (self):
        refunds = run("cbank refund --list")
        assert not refunds
        requests = run("cbank request --project grail --resource spam --amount 1000")
        allocations = run("cbank allocation --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        holds = run("cbank hold --allocation %s --amount 100" % allocations[0].id)
        charges = run("cbank charge --hold %s --amount 50" % holds[0].id)
        new_refunds = run("cbank refund --charge %s --amount 25" % charges[0].id)
        list_refunds = run("cbank refund --list --project grail --resource spam --charge %s --hold %s --allocation %s --request %s" % (charges[0].id, holds[0].id, allocations[0].id, requests[0].id))
        assert len(list_refunds) == len(new_refunds)
        assert set(list_refunds) == set(new_refunds)
