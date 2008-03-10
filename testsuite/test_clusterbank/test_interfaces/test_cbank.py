import sys
from datetime import datetime, timedelta
from StringIO import StringIO

import clusterbank.model
from clusterbank.model import Request, Allocation, Hold, Charge, Refund
from clusterbank.interfaces.cbank import main, parse_directive

def run (command):
    argv = command.split()
    return main(argv)

def parse_id (entity_str):
    return int(entity_str.split()[0])

def parse_entity (cls, entity_str):
    id = parse_id(entity_str)
    return cls.query.filter(cls.id==id).one()

def parse_entities (cls, stdout=None):
    if stdout is None:
        stdout = sys.stdout
    position = stdout.tell()
    stdout.seek(0)
    entity_strs = stdout.readlines()
    stdout.seek(position)
    return [parse_entity(cls, entity_str) for entity_str in entity_strs]

def clear_stdout (stdout=None):
    if stdout is None:
        stdout = sys.stdout
    stdout.seek(0)
    stdout.truncate()


class ScriptTester (object):
    
    def setup (self):
        clusterbank.model.metadata.create_all()
        self._stdout = sys.stdout
        sys.stdout = StringIO()
    
    def teardown (self):
        sys.stdout.close()
        sys.stdout = self._stdout
        clusterbank.model.Session.remove()
        clusterbank.model.metadata.drop_all()


def test_parse_directive ():
    assert parse_directive("req") == "request"
    assert parse_directive("allo") == "allocation"
    assert parse_directive("ref") == "refund"
    assert parse_directive("re") == "re"


class TestMain (ScriptTester):
    
    def test_request (self):
        run("cbank request --project grail --resource spam --amount 1000 --start 2007-01-01 --comment testing")
        requests = parse_entities(Request)
        assert len(requests) == 1
        request = requests[0]
        assert request.id is not None
        assert datetime.now() - request.datetime < timedelta(minutes=1)
        assert request.project.name == "grail"
        assert request.resource.name == "spam"
        assert request.amount == 1000
        assert request.start == datetime(year=2007, month=1, day=1)
        assert request.comment == "testing"
    
    def test_request_list_empty (self):
        run("cbank request --list")
        clear_stdout()
        requests = parse_entities(Request)
        assert not list(requests)
    
    def test_request_list (self):
        run("cbank request --project grail --resource spam --amount 1000")
        requests = parse_entities(Request)
        clear_stdout()
        run("cbank request --list --request %i --project grail --resource spam" % requests[0].id)
        listed_requests = parse_entities(Request)
        assert len(list(listed_requests)) == len(requests)
        assert set(listed_requests) == set(requests)
    
    def test_allocation (self):
        run("cbank request --project grail --resource spam --amount 1000 --start 2007-01-01 --comment testing")
        requests = parse_entities(Request)
        clear_stdout()
        run("cbank allocation --amount 700 --project grail --resource spam --request %i --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        allocations = parse_entities(Allocation)
        assert len(allocations) == 1
        allocation = allocations[0]
        assert allocation.id is not None
        assert datetime.now() - allocation.datetime < timedelta(minutes=1)
        assert allocation.project.name == "grail"
        assert allocation.resource.name == "spam"
        assert allocation.start == datetime(year=2007, month=1, day=1)
        assert allocation.expiration == datetime(year=2008, month=1, day=1)
    
    def test_allocation_list_empty (self):
        run("cbank allocation --list")
        allocations = parse_entities(Allocation)
        assert not list(allocations)
    
    def test_allocation_list (self):
        run("cbank allocation --project grail --resource spam --amount 700 --start 2007-01-01 --expiration 2008-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank allocation --list --allocation %i --project grail --resource spam" % allocations[0].id)
        listed_allocations = parse_entities(Allocation)
        assert len(list(listed_allocations)) == len(allocations)
        assert set(listed_allocations) == set(allocations)
    
    def test_hold (self):
        run("cbank allocation --project grail --resource spam --amount 700 --start 2007-01-01 --expiration 2008-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank hold --allocation %i --amount 100 --comment testing" % allocations[0].id)
        holds = parse_entities(Hold)
        assert len(holds) == 1
        hold = holds[0]
        assert hold.id is not None
        assert datetime.now() - hold.datetime < timedelta(minutes=1)
        assert hold.allocation is allocations[0]
        assert hold.amount == 100
        assert hold.comment == "testing"
    
    def test_hold_user (self):
        run("cbank allocation --project grail --resource spam --amount 700 --start 2007-01-01 --expiration 2008-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank hold --user monty --allocation %i --amount 100 --comment testing" % allocations[0].id)
        holds = parse_entities(Hold)
        assert len(holds) == 1
        hold = holds[0]
        assert hold.user.name == "monty"
    
    def test_hold_distributed (self):
        run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2020-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2020-01-01")
        allocations.extend(parse_entities(Allocation))
        clear_stdout()
        run("cbank hold --project grail --resource spam --amount 150 --comment testing")
        holds = parse_entities(Hold)
        assert len(holds) == 2
        assert sum(hold.amount for hold in holds) == 150
        for hold in holds:
            assert hold.allocation in allocations
    
    def test_hold_list_empty (self):
        run("cbank hold --list")
        holds = parse_entities(Hold)
        clear_stdout()
        assert not list(holds)
    
    def test_hold_list (self):
        run("cbank allocation --project grail --resource spam --amount 700 --start 2007-01-01 --expiration 2008-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank hold --allocation %i --amount 100" % allocations[0].id)
        holds = parse_entities(Hold)
        clear_stdout()
        run("cbank hold --list --hold %i --project grail --resource spam --allocation %i" % (holds[0].id, allocations[0].id))
        listed_holds = parse_entities(Hold)
        assert len(listed_holds) == len(holds)
        assert set(listed_holds) == set(holds)
    
    def test_hold_release (self):
        run("cbank allocation --project grail --resource spam --amount 200 --start 2007-01-01 --expiration 2008-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank hold --allocation %i --amount 100 --comment testing" % allocations[0].id)
        holds = parse_entities(Hold)
        clear_stdout()
        run("cbank release --allocation %i" % allocations[0].id)
        released_holds = parse_entities(Hold)
        clear_stdout()
        assert set(released_holds) == set(holds)
        for hold in released_holds:
            assert not hold.active
        run("cbank hold --list --allocation %i" % allocations[0].id)
        remaining_holds = parse_entities(Hold)
        assert not remaining_holds
    
    def test_charge (self):
        run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank charge --allocation %i --amount 50" % allocations[0].id)
        charges = parse_entities(Charge)
        assert len(charges) == 1
        charge = charges[0]
        assert charge.id is not None
        assert datetime.now() - charge.datetime < timedelta(minutes=1)
        assert charge.amount == 50
        assert charge.allocation is allocations[0]
    
    def test_charge_user (self):
        run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank charge --user monty --allocation %i --amount 50" % allocations[0].id)
        charges = parse_entities(Charge)
        assert len(charges) == 1
        charge = charges[0]
        assert charge.user.name == "monty"
    
    def test_charge_distributed (self):
        run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2020-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2020-01-01")
        allocations.extend(parse_entities(Allocation))
        clear_stdout()
        run("cbank charge --project grail --resource spam --amount 150 --comment testing")
        charges = parse_entities(Charge)
        assert len(charges) == 2
        assert sum(charge.amount for charge in charges)
        for charge in charges:
            assert charge.allocation in allocations
            assert charge.comment == "testing"
    
    def test_charge_list_empty (self):
        run("cbank charge --list")
        charges = parse_entities(Charge)
        assert not list(charges)
    
    def test_charge_list (self):
        run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank charge --allocation %i --amount 50" % allocations[0].id)
        charges = parse_entities(Charge)
        clear_stdout()
        run("cbank charge --list --charge %i --project grail --resource spam --allocation %i" % (charges[0].id, allocations[0].id))
        listed_charges = parse_entities(Charge)
        assert len(list(listed_charges)) == len(charges)
        assert set(listed_charges) == set(charges)
    
    def test_refund (self):
        run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank charge --allocation %i --amount 50" % allocations[0].id)
        charges = parse_entities(Charge)
        clear_stdout()
        run("cbank refund --charge %i --amount 25 --comment testing" % charges[0].id)
        refunds = parse_entities(Refund)
        assert len(refunds) == 1
        refund = refunds[0]
        assert refund.id is not None
        assert refund.charge is charges[0]
        assert refund.amount == 25
        assert refund.comment == "testing"
    
    def test_refund_list_empty (self):
        run("cbank refund --list")
        refunds = parse_entities(Refund)
        assert not list(refunds)
    
    def test_refund_list (self):
        run("cbank allocation --project grail --resource spam --amount 100 --start 2007-01-01 --expiration 2008-01-01")
        allocations = parse_entities(Allocation)
        clear_stdout()
        run("cbank charge --allocation %i --amount 50" % allocations[0].id)
        charges = parse_entities(Charge)
        clear_stdout()
        run("cbank refund --charge %i --amount 25" % charges[0].id)
        refunds = parse_entities(Refund)
        clear_stdout()
        run("cbank refund --list --refund %i --project grail --resource spam --charge %i --allocation %i" % (refunds[0].id, charges[0].id, allocations[0].id))
        listed_refunds = parse_entities(Refund)
        clear_stdout()
        assert len(list(listed_refunds)) == len(refunds)
        assert set(listed_refunds) == set(refunds)
