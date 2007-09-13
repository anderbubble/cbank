import os

import elixir

from clusterbank import models, scripting
from clusterbank.upstream import userbase
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
        elixir.create_all()
    
    def teardown (self):
        elixir.objectstore.clear()
        elixir.drop_all()

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
            run("cb-admin user1 --list extraargument")
        except scripting.ExtraArguments:
            pass
        else:
            assert False

    def test_grant_some (self):
        permissions = list(run("cb-admin user1 --grant request,lien --list"))
        assert len(permissions) == 2
        for permission in ("request", "lien"):
            assert permission in permissions

    def test_grant_all (self):
        permissions = list(run("cb-admin user1 --grant all --list"))
        assert len(permissions) == 5
        for permission in ("request", "allocate", "lien", "charge", "refund"):
            assert permission in permissions

    def test_revoke_some (self):
        run("cb-admin user1 --grant all")
        permissions = list(run("cb-admin user1 --revoke request,lien --list"))
        assert len(permissions) == 3
        for permission in ("request", "lien"):
            assert permission not in permissions

    def test_revoke_all (self):
        run("cb-admin user1 --grant all")
        permissions = list(run("cb-admin user1 --revoke all --list"))
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
        run("cb-admin user1 --grant request")
        for command in (
            "cb-request",
            "cb-request user1",
            "cb-request user1 project1",
            "cb-request user1 project1 resource1",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        for command in (
            "cb-request user1 project1 resource1 notatime",
            "cb-request user1 project1 notaresource 1000",
            "cb-request user1 notaproject resource1 1000",
            "cb-request notauser project1 resource1 1000",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
        try:
            run("cb-request user1 project1 resource1 1000 extra")
        except scripting.ExtraArguments:
            pass
        else:
            assert False
    
    def test_new_request (self):
        run("cb-admin user1 --grant request")
        requests = run("cb-request user1 project1 resource1 1000 --start 2007-01-01 --explanation testing")
        assert len(requests) == 1
        request = requests[0]
        assert request.poster.name == "user1"
        assert request.project.name == "project1"
        assert request.resource.name == "resource1"
        assert request.time == 1000
        assert request.start.strftime("%d-%m-%Y") == "01-01-2007"
        assert request.explanation == "testing"
    
    def test_list_requests (self):
        requests = list(run("cb-request user1 --list"))
        assert len(requests) == 0
        
        run("cb-admin user1 --grant request")
        new_requests = list(run("cb-request user1 project1 resource1 1000"))
        list_requests = list(run("cb-request user1 --list --project project1"))
        assert len(list_requests) == 1
        for request in list_requests:
            assert request in new_requests
    
    def test_extra_list_arguments (self):
        try:
            run("cb-request user1 --list extra")
        except scripting.ExtraArguments:
            pass
        else:
            assert False


class TestAllocation (ScriptTester):
    
    def test_create (self):
        run("cb-admin user1 --grant request,allocate")
        run("cb-request user1 project1 resource1 1000")
        for command in (
            "cb-allocate",
            "cb-allocate user1",
            "cb-allocate user1 1",
            "cb-allocate user1 1 2007-01-01",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        for command in (
            "cb-allocate doesnotexist",
            "cb-allocate user1 0",
            "cb-allocate user1 1 notadate",
            "cb-allocate user1 1 2007-01-01 notadate",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
        allocations = list(run("cb-allocate user1 1 2007-01-01 2008-01-01 --credit-limit 100"))
        assert len(allocations) == 1
    
    def test_create_permission (self):
        run("cb-admin user1 --grant request")
        run("cb-request user1 project1 resource1 1000")
        try:
            run("cb-allocate user1 1 2007-01-01 2008-01-01")
        except scripting.NotPermitted:
            pass
        else:
            assert False
    
    def test_invalid_credit (self):
        run("cb-admin user1 --grant request,allocate")
        run("cb-request user1 project1 resource1 1000")
        try:
            run("cb-allocate user1 1 2007-01-01 2008-01-01 --credit -100")
        except scripting.InvalidArgument:
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
        run("cb-admin user1 --grant request")
        run("cb-request user1 project1 resource1 1000")
        for command in (
            "cb-allocate user1 --request 1 --start 2007-01-01 --expiration 2008-01-01 extra",
            "cb-allocate user1 --list extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_list (self):
        requests = list(run("cb-allocate user1 --list"))
        assert not requests
    
    def test_list_specific (self):
        run("cb-admin user1 --grant request,allocate")
        requests = list(run("cb-request user1 project1 resource1 1000"))
        run("cb-allocate user1 %s --start 2007-01-01 --expiration 2008-01-01" % requests[0].id)
        allocations = list(run("cb-allocate user1 --list --request 1 --project project1 --resource resource1"))
        print allocations, ":"
        for allocation in allocations:
            print allocation.project, allocation.resource
        assert len(allocations) == 1


class TestLien (ScriptTester):
    
    def test_required_arguments (self):
        run("cb-admin user1 --grant request,allocate,lien")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        for command in (
            "cb-lien",
            "cb-lien user1",
            "cb-lien user1 1",
            "cb-lien user1 --project project1 --resource resource1",
            "cb-lien user1 --project project1 --time 100",
            "cb-lien user1 --resource resource1 --time 100",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        
        for command in (
            "cb-lien user1 1 notatime",
            "cb-lien user1 notanallocation 100",
            "cb-lien notauser 1 100",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
    
    def test_list (self):
        liens = list(run("cb-lien user1 --list"))
        assert not liens
        
        run("cb-admin user1 --grant request,allocate,lien")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        run("cb-lien user1 1 100")
        liens = list(run("cb-lien user1 --list"))
        assert len(liens) == 1
        liens = list(run("cb-lien user1 --list --project project1"))
        assert len(liens) == 1
        liens = list(run("cb-lien user1 --list --resource resource1"))
        assert len(liens) == 1
        liens = list(run("cb-lien user1 --list --allocation 1"))
        assert len(liens) == 1
    
    def test_no_extra_arguments (self):
        run("cb-admin user1 --grant request,allocate,lien")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        for command in (
            "cb-lien user1 --list extra",
            "cb-lien user1 --allocation 1 --time 100 extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_standard_lien (self):
        run("cb-admin user1 --grant request,allocate")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        try:
            run("cb-lien user1 --allocation 1 --time 100 --explanation testing")
        except scripting.NotPermitted:
            pass
        else:
            assert False
        run("cb-admin user1 --grant lien")
        liens = list(run("cb-lien user1 --allocation 1 --time 100 --explanation testing"))
        assert len(liens) == 1
    
    def test_smart_lien (self):
        run("cb-admin user1 --grant request,allocate,lien")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        liens = list(run("cb-lien user1 --time 100 --project project1 --resource resource1 --explanation testing"))
        assert len(liens) == 1
    
    def test_invalid_lien (self):
        run("cb-admin user1 --grant request,allocate,lien")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        try:
            run("cb-lien user1 --allocation 1 --time -100 --explanation testing")
        except scripting.InvalidArgument:
            pass
        else:
            assert False


class TestCharge (ScriptTester):
    
    def test_required_arguments (self):
        run("cb-admin user1 --grant request,allocate,lien")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        run("cb-lien user1 1 100")
        for command in (
            "cb-charge",
            "cb-charge user1",
            "cb-charge user1 1",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        
        for command in (
            "cb-charge user1 1 notatime",
            "cb-charge user1 notanallocation 75",
            "cb-charge notauser 1 75",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
    
    def test_extra_arguments (self):
        run("cb-admin user1 --grant request,allocate,lien")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        run("cb-lien user1 1 100")
        for command in (
            "cb-charge user1 1 50 extra",
            "cb-charge user1 --list extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_list (self):
        run("cb-admin user1 --grant request,allocate,lien,charge")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        run("cb-lien user1 1 100")
        charges = list(run("cb-charge user1 --list"))
        assert not charges
        run("cb-charge user1 1 50")
        charges = list(run("cb-charge user1 --list --project project1 --resource resource1 --lien 1"))
        assert len(charges) == 1
    
    def test_standard_charge (self):
        run("cb-admin user1 --grant request,allocate,lien")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        run("cb-lien user1 1 100")
        try:
            run("cb-charge user1 1 50")
        except scripting.NotPermitted:
            pass
        else:
            assert False
        run("cb-admin user1 --grant charge")
        charges = list(run("cb-charge user1 1 50"))
        assert len(charges) == 1
    
    def test_invalid_time (self):
        run("cb-admin user1 --grant request,allocate,lien,charge")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        run("cb-lien user1 1 100")
        try:
            run("cb-charge user1 1 --time -50")
        except scripting.InvalidArgument:
            pass
        else:
            assert False


class TestRefund (ScriptTester):
    
    def test_required_arguments (self):
        run("cb-admin user1 --grant request,allocate,lien,charge")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        run("cb-lien user1 1 100")
        run("cb-charge user1 1 50")
        for command in (
            "cb-refund",
            "cb-refund user1",
            "cb-refund user1 1",
        ):
            try:
                run(command)
            except scripting.MissingArgument:
                pass
            else:
                assert False
        
        for command in (
            "cb-refund user1 1 notatime",
            "cb-refund user1 notacharge 25",
            "cb-refund notauser 1 25",
        ):
            try:
                run(command)
            except scripting.InvalidArgument:
                pass
            else:
                assert False
    
    def test_extra_arguments (self):
        run("cb-admin user1 --grant request,allocate,lien,charge")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        run("cb-lien user1 1 100")
        run("cb-charge user1 1 50")
        for command in (
            "cb-refund user1 1 25 extra",
            "cb-refund user1 --list extra",
        ):
            try:
                run(command)
            except scripting.ExtraArguments:
                pass
            else:
                assert False
    
    def test_list (self):
        run("cb-admin user1 --grant request,allocate,lien,charge")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        run("cb-lien user1 1 100")
        run("cb-charge user1 1 50")
        refunds = list(run("cb-refund user1 --list"))
        assert not refunds
        run("cb-admin user1 --grant refund")
        run("cb-refund user1 1 25")
        refunds = list(run("cb-refund user1 --list --project project1 --resource resource1 --charge 1 --lien 1"))
        assert len(refunds) == 1
    
    def test_standard_refund (self):
        run("cb-admin user1 --grant request,allocate,lien,charge")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        run("cb-lien user1 1 100")
        run("cb-charge user1 1 50")
        run("cb-admin user1 --grant refund")
        refunds = list(run("cb-refund user1 1 25 --explanation testing"))
        assert len(refunds) == 1
    
    def test_invalid_time (self):
        run("cb-admin user1 --grant request,allocate,lien,charge,refund")
        run("cb-request user1 project1 resource1 1000")
        run("cb-allocate user1 1 2007-01-01 2008-01-01")
        run("cb-lien user1 1 100")
        run("cb-charge user1 1 50")
        try:
            run("cb-refund user1 1 --time -25")
        except scripting.InvalidArgument:
            pass
        else:
            assert False
