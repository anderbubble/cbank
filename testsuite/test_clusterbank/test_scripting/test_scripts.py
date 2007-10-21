import os

import clusterbank.model
from clusterbank import scripting
from clusterbank.scripting import admin, request, allocation, lien, charge, refund

def run (command):
    argv = command.split()
    exe = argv[0]
    argv[0] = os.path.abspath(argv[0])
    if exe == "cb-admin":
        return admin.run(argv)
    elif exe == "cb-request":
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
        clusterbank.model.Session.clear()
        clusterbank.model.metadata.drop_all()

class TestAdmin (ScriptTester):
    
    def test_without_user (self):
        for command in (
            "cb-admin",
            "cb-admin --list",
            "cb-admin --grant all",
            "cb-admin --revoke all",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
    
    def test_invalid_user (self):
        try:
            run("cb-admin doesnotexist")
        except scripting.InvalidArgument:
            pass
        else:
            assert False
    
    def test_extra_arguments (self):
        try:
            run("cb-admin Monty --list extraargument")
        except scripting.ExtraArguments:
            pass
        else:
            assert False

    def test_grant_some (self):
        permissions = list(run("cb-admin Monty --grant request,lien --list"))
        assert len(permissions) == 2
        for permission in ("request", "lien"):
            assert permission in permissions

    def test_grant_all (self):
        permissions = list(run("cb-admin Monty --grant all --list"))
        assert len(permissions) == 5
        for permission in ("request", "allocate", "lien", "charge", "refund"):
            assert permission in permissions

    def test_revoke_some (self):
        run("cb-admin Monty --grant all")
        permissions = list(run("cb-admin Monty --revoke request,lien --list"))
        assert len(permissions) == 3
        for permission in ("request", "lien"):
            assert permission not in permissions

    def test_revoke_all (self):
        run("cb-admin Monty --grant all")
        permissions = list(run("cb-admin Monty --revoke all --list"))
        assert not permissions
    

class TestRequest (ScriptTester):
    
    def test_without_user (self):
        for command in (
            "cb-request",
            "cb-request --list",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
    
    def test_invalid_user (self):
        try:
            run("cb-request doesnotexist")
        except scripting.InvalidArgument:
            pass
        else:
            assert False
    
    def test_arguments (self):
        run("cb-admin Monty --grant request")
        for command in (
            "cb-request",
            "cb-request Monty",
            "cb-request Monty grail",
            "cb-request Monty grail spam",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        for command in (
            "cb-request Monty grail spam notatime",
            "cb-request Monty grail notaresource 1000",
            "cb-request Monty notaproject spam 1000",
            "cb-request notauser grail spam 1000",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
        try:
            run("cb-request Monty grail spam 1000 extra")
        except scripting.ExtraArguments:
            pass
        else:
            assert False
    
    def test_new_request (self):
        run("cb-admin Monty --grant request")
        requests = run("cb-request Monty grail spam 1000 --start 2007-01-01 --comment testing")
        assert len(requests) == 1
        request = requests[0]
        assert request.poster.name == "Monty"
        assert request.project.name == "grail"
        assert request.resource.name == "spam"
        assert request.time == 1000
        assert request.start.strftime("%d-%m-%Y") == "01-01-2007"
        assert request.comment == "testing"
    
    def test_list_requests (self):
        requests = list(run("cb-request Monty --list"))
        assert len(requests) == 0
        
        run("cb-admin Monty --grant request")
        new_requests = list(run("cb-request Monty grail spam 1000"))
        list_requests = list(run("cb-request Monty --list --project grail"))
        assert len(list_requests) == 1
        for request in list_requests:
            assert request in new_requests
    
    def test_extra_list_arguments (self):
        try:
            run("cb-request Monty --list extra")
        except scripting.ExtraArguments:
            pass
        else:
            assert False


class TestAllocation (ScriptTester):
    
    def test_create (self):
        run("cb-admin Monty --grant request,allocate")
        run("cb-request Monty grail spam 1000")
        for command in (
            "cb-allocate",
            "cb-allocate Monty",
            "cb-allocate Monty 1",
            "cb-allocate Monty 1 2007-01-01",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        for command in (
            "cb-allocate doesnotexist",
            "cb-allocate Monty 0",
            "cb-allocate Monty 1 notadate",
            "cb-allocate Monty 1 2007-01-01 notadate",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
        allocations = list(run("cb-allocate Monty 1 2007-01-01 2008-01-01 --credit-limit 100"))
        assert len(allocations) == 1
    
    def test_create_permission (self):
        run("cb-admin Monty --grant request")
        run("cb-request Monty grail spam 1000")
        try:
            run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        except clusterbank.model.User.NotPermitted:
            pass
        else:
            assert False
    
    def test_invalid_credit (self):
        run("cb-admin Monty --grant request,allocate")
        run("cb-request Monty grail spam 1000")
        try:
            run("cb-allocate Monty 1 2007-01-01 2008-01-01 --credit -100")
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
        run("cb-admin Monty --grant request")
        run("cb-request Monty grail spam 1000")
        for command in (
            "cb-allocate Monty --request 1 --start 2007-01-01 --expiration 2008-01-01 extra",
            "cb-allocate Monty --list extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_list (self):
        requests = list(run("cb-allocate Monty --list"))
        assert not requests
    
    def test_list_specific (self):
        run("cb-admin Monty --grant request,allocate")
        requests = list(run("cb-request Monty grail spam 1000"))
        run("cb-allocate Monty %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        allocations = list(run("cb-allocate Monty --list --request 1 --project grail --resource spam"))
        assert len(allocations) == 1


class TestLien (ScriptTester):
    
    def test_required_arguments (self):
        run("cb-admin Monty --grant request,allocate,lien")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        for command in (
            "cb-lien",
            "cb-lien Monty",
            "cb-lien Monty 1",
            "cb-lien Monty --project grail --resource spam",
            "cb-lien Monty --project grail --time 100",
            "cb-lien Monty --resource spam --time 100",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        
        for command in (
            "cb-lien Monty 1 notatime",
            "cb-lien Monty notanallocation 100",
            "cb-lien notauser 1 100",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
    
    def test_list (self):
        liens = list(run("cb-lien Monty --list"))
        assert not liens
        
        run("cb-admin Monty --grant request,allocate,lien")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        run("cb-lien Monty 1 100")
        liens = list(run("cb-lien Monty --list"))
        assert len(liens) == 1
        liens = list(run("cb-lien Monty --list --project grail"))
        assert len(liens) == 1
        liens = list(run("cb-lien Monty --list --resource spam"))
        assert len(liens) == 1
        liens = list(run("cb-lien Monty --list --allocation 1"))
        assert len(liens) == 1
    
    def test_no_extra_arguments (self):
        run("cb-admin Monty --grant request,allocate,lien")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        for command in (
            "cb-lien Monty --list extra",
            "cb-lien Monty --allocation 1 --time 100 extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_standard_lien (self):
        run("cb-admin Monty --grant request,allocate")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        try:
            run("cb-lien Monty --allocation 1 --time 100 --comment testing")
        except clusterbank.model.User.NotPermitted:
            pass
        else:
            assert False
        run("cb-admin Monty --grant lien")
        liens = list(run("cb-lien Monty --allocation 1 --time 100 --comment testing"))
        assert len(liens) == 1
    
    def test_smart_lien (self):
        run("cb-admin Monty --grant request,allocate,lien")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        liens = list(run("cb-lien Monty --time 100 --project grail --resource spam --comment testing"))
        assert len(liens) == 1
    
    def test_invalid_lien (self):
        run("cb-admin Monty --grant request,allocate,lien")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        try:
            run("cb-lien Monty --allocation 1 --time -100 --comment testing")
        except ValueError:
            pass
        else:
            assert False


class TestCharge (ScriptTester):
    
    def test_required_arguments (self):
        run("cb-admin Monty --grant request,allocate,lien")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        run("cb-lien Monty 1 100")
        for command in (
            "cb-charge",
            "cb-charge Monty",
            "cb-charge Monty 1",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        
        for command in (
            "cb-charge Monty 1 notatime",
            "cb-charge Monty notanallocation 75",
            "cb-charge notauser 1 75",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
    
    def test_extra_arguments (self):
        run("cb-admin Monty --grant request,allocate,lien")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        run("cb-lien Monty 1 100")
        for command in (
            "cb-charge Monty 1 50 extra",
            "cb-charge Monty --list extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_list (self):
        run("cb-admin Monty --grant request,allocate,lien,charge")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        run("cb-lien Monty 1 100")
        charges = list(run("cb-charge Monty --list"))
        assert not charges
        run("cb-charge Monty 1 50")
        charges = list(run("cb-charge Monty --list --project grail --resource spam --lien 1"))
        assert len(charges) == 1
    
    def test_standard_charge (self):
        run("cb-admin Monty --grant request,allocate,lien")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        run("cb-lien Monty 1 100")
        try:
            run("cb-charge Monty 1 50")
        except clusterbank.model.User.NotPermitted:
            pass
        else:
            assert False
        run("cb-admin Monty --grant charge")
        charges = list(run("cb-charge Monty 1 50"))
        assert len(charges) == 1
    
    def test_invalid_time (self):
        run("cb-admin Monty --grant request,allocate,lien,charge")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        run("cb-lien Monty 1 100")
        try:
            run("cb-charge Monty 1 --time -50")
        except ValueError:
            pass
        else:
            assert False


class TestRefund (ScriptTester):
    
    def test_required_arguments (self):
        run("cb-admin Monty --grant request,allocate,lien,charge")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        run("cb-lien Monty 1 100")
        run("cb-charge Monty 1 50")
        for command in (
            "cb-refund",
            "cb-refund Monty",
            "cb-refund Monty 1",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        
        for command in (
            "cb-refund Monty 1 notatime",
            "cb-refund Monty notacharge 25",
            "cb-refund notauser 1 25",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
    
    def test_extra_arguments (self):
        run("cb-admin Monty --grant request,allocate,lien,charge")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        run("cb-lien Monty 1 100")
        run("cb-charge Monty 1 50")
        for command in (
            "cb-refund Monty 1 25 extra",
            "cb-refund Monty --list extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_list (self):
        run("cb-admin Monty --grant request,allocate,lien,charge")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        run("cb-lien Monty 1 100")
        run("cb-charge Monty 1 50")
        refunds = list(run("cb-refund Monty --list"))
        assert not refunds
        run("cb-admin Monty --grant refund")
        run("cb-refund Monty 1 25")
        refunds = list(run("cb-refund Monty --list --project grail --resource spam --charge 1 --lien 1"))
        assert len(refunds) == 1
    
    def test_standard_refund (self):
        run("cb-admin Monty --grant request,allocate,lien,charge")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        run("cb-lien Monty 1 100")
        run("cb-charge Monty 1 50")
        run("cb-admin Monty --grant refund")
        refunds = list(run("cb-refund Monty 1 25 --comment testing"))
        assert len(refunds) == 1
    
    def test_invalid_time (self):
        run("cb-admin Monty --grant request,allocate,lien,charge,refund")
        run("cb-request Monty grail spam 1000")
        run("cb-allocate Monty 1 2007-01-01 2008-01-01")
        run("cb-lien Monty 1 100")
        run("cb-charge Monty 1 50")
        try:
            run("cb-refund Monty 1 --time -25")
        except ValueError:
            pass
        else:
            assert False
