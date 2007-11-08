import os

import clusterbank.model
from clusterbank import scripting
from clusterbank.scripting import request, allocation, hold, charge, refund

def run (command):
    argv = command.split()
    exe = argv[0]
    argv[0] = os.path.abspath(argv[0])
    if exe == "cb-request":
        return request.run(argv)
    elif exe == "cb-allocate":
        return allocation.run(argv)
    elif exe == "cb-hold":
        return hold.run(argv)
    elif exe == "cb-charge":
        return charge.run(argv)
    elif exe == "cb-refund":
        return refund.run(argv)
    else: assert not "Invalid command executable."


class ScriptTester (object):
    
    def setup (self):
        clusterbank.model.metadata.create_all()
    
    def teardown (self):
        clusterbank.model.Session.remove()
        clusterbank.model.metadata.drop_all()


class TestRequest (ScriptTester):
    
    def test_required_options (self):
        for command in ("cb-request", "cb-request --project grail", "cb-request --project grail --resource spam"):
            try:
                retval = run(command)
            except Exception:
                pass
            else:
                assert not "didn't raise exception"
    
    def test_new_request (self):
        requests = run("cb-request --project grail --resource spam --amount 1000 --start 2007-01-01 --comment testing")
        assert len(requests) == 1
        request = requests[0]
        assert request.project.name == "grail"
        assert request.resource.name == "spam"
        assert request.amount == 1000
        assert request.start.strftime("%d-%m-%Y") == "01-01-2007"
        assert request.comment == "testing"
        assert request in list(run("cb-request --list --project grail"))
    
    def test_list_requests (self):
        requests = list(run("cb-request --list"))
        assert len(requests) == 0
        new_requests = list(run("cb-request --project grail --resource spam --amount 1000"))
        list_requests = list(run("cb-request --list --project grail"))
        assert len(list_requests) == 1
        for request in list_requests:
            assert request in new_requests


class TestAllocation (ScriptTester):
    
    def test_create (self):
        run("cb-request --project grail --resource spam --amount 1000")
        for command in (
            "cb-allocate",
            "cb-allocate --request 1",
            "cb-allocate --request 1 --start 2007-01-01",
        ):
            try:
                run(command)
            except Exception:
                pass
            else:
                assert False
        for command in (
            "cb-allocate --request 1",
            "cb-allocate --request 1 --start 2007-01-01",
        ):
            try:
                run(command)
            except Exception:
                pass
            else:
                assert False
        allocations = list(run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01 --credit-limit 100"))
        assert len(allocations) == 1
    
    def test_invalid_credit (self):
        run("cb-request --project grail --resource spam --amount 1000")
        try:
            run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01 --credit -100")
        except ValueError:
            pass
        else:
            assert False
    
    def test_extra_arguments (self):
        run("cb-request --project grail --resource spam --amount 1000")
        for command in (
            "cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01 extra",
            "cb-allocate --list extra",
        ):
            try:
                run(command)
            except Exception:
                pass
            else:
                assert False
    
    def test_list (self):
        requests = list(run("cb-allocate --list"))
        assert not requests
    
    def test_list_specific (self):
        requests = list(run("cb-request --project grail --resource spam --amount 1000"))
        run("cb-allocate --request %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        allocations = list(run("cb-allocate --list --request 1 --project grail --resource spam"))
        assert len(allocations) == 1


class TestHold (ScriptTester):
    
    def test_required_options (self):
        import traceit
        traceit.trace_process(scope="clusterbank.", log="trace.log")
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        for command in (
            "cb-hold",
            "cb-hold --allocation 1",
            "cb-hold --project grail --resource spam",
            "cb-hold --project grail --amount 100",
            "cb-hold --resource spam --amount 100",
        ):
            try:
                run(command)
            except Exception:
                pass
            else:
                assert False
    
    def test_list (self):
        holds = list(run("cb-hold --list"))
        assert not holds
        
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        run("cb-hold --allocation 1 --amount 100")
        holds = list(run("cb-hold --list"))
        assert len(holds) == 1
        holds = list(run("cb-hold --list --project grail"))
        assert len(holds) == 1
        holds = list(run("cb-hold --list --resource spam"))
        assert len(holds) == 1
        holds = list(run("cb-hold --list --allocation 1"))
        assert len(holds) == 1
    
    def test_standard_hold (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        holds = list(run("cb-hold --allocation 1 --amount 100 --comment testing"))
        assert len(holds) == 1
    
    def test_smart_hold (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        holds = list(run("cb-hold --amount 100 --project grail --resource spam --comment testing"))
        assert len(holds) == 1
    
    def test_invalid_hold (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        try:
            run("cb-hold --allocation 1 --amount -100 --comment testing")
        except ValueError:
            pass
        else:
            assert False


class TestCharge (ScriptTester):
    
    def test_required_options (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        run("cb-hold --allocation 1 --amount 100")
        for command in (
            "cb-charge",
            "cb-charge --hold 1",
        ):
            try:
                run(command)
            except Exception:
                pass
            else:
                assert False
    
    def test_extra_arguments (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        run("cb-hold --allocation 1 --amount 100")
        for command in (
            "cb-charge --hold 1 --amount 50 extra",
            "cb-charge --list extra",
        ):
            try:
                run(command)
            except Exception:
                pass
            else:
                assert False
    
    def test_list (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        run("cb-hold --allocation 1 --amount 100")
        charges = list(run("cb-charge --list"))
        assert not charges
        run("cb-charge --hold 1 --amount 50")
        charges = list(run("cb-charge --list --project grail --resource spam --hold 1"))
        assert len(charges) == 1
    
    def test_standard_charge (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        run("cb-hold --allocation 1 --amount 100")
        charges = list(run("cb-charge --hold 1 --amount 50"))
        assert len(charges) == 1
    
    def test_invalid_amount (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        run("cb-hold --allocation 1 --amount 100")
        try:
            run("cb-charge --hold 1 --amount -50")
        except ValueError:
            pass
        else:
            assert False


class TestRefund (ScriptTester):
    
    def test_required_options (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        run("cb-hold --allocation 1 --amount 100")
        run("cb-charge --hold 1 --amount 50")
        for command in (
            "cb-refund",
            "cb-refund --charge 1",
        ):
            try:
                run(command)
            except Exception:
                pass
            else:
                assert False
    
    def test_extra_arguments (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        run("cb-hold --allocation 1 --amount 100")
        run("cb-charge --hold 1 --amount 50")
        for command in (
            "cb-refund --charge 1 --amount 25 extra",
            "cb-refund --list extra",
        ):
            try:
                run(command)
            except Exception:
                pass
            else:
                assert False
    
    def test_list (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        run("cb-hold --allocation 1 --amount 100")
        run("cb-charge --hold 1 --amount 50")
        refunds = list(run("cb-refund --list"))
        assert not refunds
        run("cb-refund --charge 1 --amount 25")
        refunds = list(run("cb-refund --list --project grail --resource spam --charge 1 --hold 1"))
        assert len(refunds) == 1
    
    def test_standard_refund (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        run("cb-hold --allocation 1 --amount 100")
        run("cb-charge --hold 1 --amount 50")
        refunds = list(run("cb-refund --charge 1 --amount 25 --comment testing"))
        assert len(refunds) == 1
    
    def test_invalid_amount (self):
        run("cb-request --project grail --resource spam --amount 1000")
        run("cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01")
        run("cb-hold --allocation 1 --amount 100")
        run("cb-charge --hold 1 --amount 50")
        try:
            run("cb-refund --charge 1 --amount -25")
        except ValueError:
            pass
        else:
            assert False
