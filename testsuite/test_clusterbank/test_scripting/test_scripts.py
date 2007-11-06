import os

import clusterbank.model
from clusterbank import scripting
from clusterbank.scripting import request, allocation, lien, charge, refund

def run (command):
    argv = command.split()
    exe = argv[0]
    argv[0] = os.path.abspath(argv[0])
    if exe == "cb-request":
        return request.run(argv)
    elif exe == "cb-allocate":
        return allocation.run(argv)
    elif exe == "cb-lien":
        return lien.run(argv)
    elif exe == "cb-charge":
        return charge.run(argv)
    elif exe == "cb-refund":
        return refund.run(argv)
    else: assert not "Invalid command executable."


class ScriptTester (object):
    
    def setup (self):
        clusterbank.model.metadata.create_all()
    
    def teardown (self):
        clusterbank.model.Session.close()
        clusterbank.model.metadata.drop_all()


class TestRequest (ScriptTester):
    
    def test_invalid_user (self):
        try:
            run("cb-request doesnotexist")
        except scripting.InvalidArgument:
            pass
        else:
            assert False
    
    def test_arguments (self):
        for command in (
            "cb-request",
            "cb-request grail",
            "cb-request grail spam",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        for command in (
            "cb-request grail spam notatime",
            "cb-request grail notaresource 1000",
            "cb-request notaproject spam 1000",
            "cb-request notauser grail spam 1000",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
        try:
            run("cb-request grail spam 1000 extra")
        except scripting.ExtraArguments:
            pass
        else:
            assert False
    
    def test_new_request (self):
        requests = run("cb-request grail spam 1000 --start 2007-01-01 --comment testing")
        assert len(requests) == 1
        request = requests[0]
        assert request.project.name == "grail"
        assert request.resource.name == "spam"
        assert request.time == 1000
        assert request.start.strftime("%d-%m-%Y") == "01-01-2007"
        assert request.comment == "testing"
    
    def test_list_requests (self):
        requests = list(run("cb-request --list"))
        assert len(requests) == 0
        
        new_requests = list(run("cb-request grail spam 1000"))
        list_requests = list(run("cb-request --list --project grail"))
        assert len(list_requests) == 1
        for request in list_requests:
            assert request in new_requests
    
    def test_extra_list_arguments (self):
        try:
            run("cb-request --list extra")
        except scripting.ExtraArguments:
            pass
        else:
            assert False


class TestAllocation (ScriptTester):
    
    def test_create (self):
        run("cb-request grail spam 1000")
        for command in (
            "cb-allocate",
            "cb-allocate",
            "cb-allocate 1",
            "cb-allocate 1 2007-01-01",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        for command in (
            "cb-allocate doesnotexist",
            "cb-allocate 0",
            "cb-allocate 1 notadate",
            "cb-allocate 1 2007-01-01 notadate",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
        allocations = list(run("cb-allocate 1 2007-01-01 2008-01-01 --credit-limit 100"))
        assert len(allocations) == 1
    
    def test_invalid_credit (self):
        run("cb-request grail spam 1000")
        try:
            run("cb-allocate 1 2007-01-01 2008-01-01 --credit -100")
        except ValueError:
            pass
        else:
            assert False
    
    def test_invalid_user (self):
        try:
            run("cb-allocate doesnotexist")
        except scripting.InvalidArgument:
            pass
        else:
            assert False
    
    def test_extra_arguments (self):
        run("cb-request grail spam 1000")
        for command in (
            "cb-allocate --request 1 --start 2007-01-01 --expiration 2008-01-01 extra",
            "cb-allocate --list extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_list (self):
        requests = list(run("cb-allocate --list"))
        assert not requests
    
    def test_list_specific (self):
        requests = list(run("cb-request grail spam 1000"))
        run("cb-allocate %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        allocations = list(run("cb-allocate --list --request 1 --project grail --resource spam"))
        assert len(allocations) == 1


class TestLien (ScriptTester):
    
    def test_required_arguments (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        for command in (
            "cb-lien",
            "cb-lien",
            "cb-lien 1",
            "cb-lien --project grail --resource spam",
            "cb-lien --project grail --time 100",
            "cb-lien --resource spam --time 100",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        
        for command in (
            "cb-lien 1 notatime",
            "cb-lien notanallocation 100",
            "cb-lien notauser 1 100",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
    
    def test_list (self):
        liens = list(run("cb-lien --list"))
        assert not liens
        
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        run("cb-lien 1 100")
        liens = list(run("cb-lien --list"))
        assert len(liens) == 1
        liens = list(run("cb-lien --list --project grail"))
        assert len(liens) == 1
        liens = list(run("cb-lien --list --resource spam"))
        assert len(liens) == 1
        liens = list(run("cb-lien --list --allocation 1"))
        assert len(liens) == 1
    
    def test_no_extra_arguments (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        for command in (
            "cb-lien --list extra",
            "cb-lien --allocation 1 --time 100 extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_standard_lien (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        liens = list(run("cb-lien --allocation 1 --time 100 --comment testing"))
        assert len(liens) == 1
    
    def test_smart_lien (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        liens = list(run("cb-lien --time 100 --project grail --resource spam --comment testing"))
        assert len(liens) == 1
    
    def test_invalid_lien (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        try:
            run("cb-lien --allocation 1 --time -100 --comment testing")
        except ValueError:
            pass
        else:
            assert False


class TestCharge (ScriptTester):
    
    def test_required_arguments (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        run("cb-lien 1 100")
        for command in (
            "cb-charge",
            "cb-charge 1",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        
        for command in (
            "cb-charge 1 notatime",
            "cb-charge notanallocation 75",
            "cb-charge notauser 1 75",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
    
    def test_extra_arguments (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        run("cb-lien 1 100")
        for command in (
            "cb-charge 1 50 extra",
            "cb-charge --list extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_list (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        run("cb-lien 1 100")
        charges = list(run("cb-charge --list"))
        assert not charges
        run("cb-charge 1 50")
        charges = list(run("cb-charge --list --project grail --resource spam --lien 1"))
        assert len(charges) == 1
    
    def test_standard_charge (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        run("cb-lien 1 100")
        charges = list(run("cb-charge 1 50"))
        assert len(charges) == 1
    
    def test_invalid_time (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        run("cb-lien 1 100")
        try:
            run("cb-charge 1 --time -50")
        except ValueError:
            pass
        else:
            assert False


class TestRefund (ScriptTester):
    
    def test_required_arguments (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        run("cb-lien 1 100")
        run("cb-charge 1 50")
        for command in (
            "cb-refund",
            "cb-refund 1",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        
        for command in (
            "cb-refund 1 notatime",
            "cb-refund notacharge 25",
            "cb-refund notauser 1 25",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
    
    def test_extra_arguments (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        run("cb-lien 1 100")
        run("cb-charge 1 50")
        for command in (
            "cb-refund 1 25 extra",
            "cb-refund --list extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_list (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        run("cb-lien 1 100")
        run("cb-charge 1 50")
        refunds = list(run("cb-refund --list"))
        assert not refunds
        run("cb-refund 1 25")
        refunds = list(run("cb-refund --list --project grail --resource spam --charge 1 --lien 1"))
        assert len(refunds) == 1
    
    def test_standard_refund (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        run("cb-lien 1 100")
        run("cb-charge 1 50")
        refunds = list(run("cb-refund 1 25 --comment testing"))
        assert len(refunds) == 1
    
    def test_invalid_time (self):
        run("cb-request grail spam 1000")
        run("cb-allocate 1 2007-01-01 2008-01-01")
        run("cb-lien 1 100")
        run("cb-charge 1 50")
        try:
            run("cb-refund 1 --time -25")
        except ValueError:
            pass
        else:
            assert False
