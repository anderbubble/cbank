import sys
import pwd
import os
from datetime import datetime, timedelta
from StringIO import StringIO
from textwrap import dedent

from sqlalchemy import create_engine

import clusterbank
import clusterbank.model
from clusterbank.model import user_by_name, project_by_name, \
    resource_by_name, user_projects, project_members, Session, User, \
    Allocation, Hold, Charge, Refund
from clusterbank.model.database import metadata
import clusterbank.upstreams.default as upstream
import clusterbank.cbank.controllers as controllers
from clusterbank.cbank.controllers import main, report_main, new_main, \
    report_users_main, report_projects_main, report_allocations_main, \
    report_holds_main, report_charges_main, new_allocation_main, \
    new_charge_main, new_hold_main, new_refund_main
from clusterbank.cbank.exceptions import UnknownCommand, \
    UnexpectedArguments, UnknownProject, MissingArgument, MissingResource, \
    NotPermitted, ValueError_, UnknownCharge

from nose.tools import assert_equal


def current_username ():
    return pwd.getpwuid(os.getuid())[0]


class FakeDateTime (object):
    
    def __init__ (self, now):
        self._now = now
    
    def __call__ (self, *args):
        return datetime(*args)
    
    def now (self):
        return self._now


class FakeFunc (object):
    
    def __init__ (self, func=lambda:None):
        self.calls = []
        self.func = func
    
    def __call__ (self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.func()


def run (func, args=None):
    if args is None:
        args = []
    real_argv = sys.argv
    real_stdout = sys.stdout
    real_stderr = sys.stderr
    try:
        sys.argv = [func.__name__] + args
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        try:
            func()
        except SystemExit, e:
            code = e.code
        else:
            code = 0
        for stdf in (sys.stdout, sys.stderr):
            stdf.flush()
            stdf.seek(0)
        return code, sys.stdout, sys.stderr
    finally:
        sys.argv = real_argv
        sys.stdout = real_stdout
        sys.stderr = real_stderr


def assert_eq_output (output, correct):
    assert output == correct, os.linesep.join([
        "incorrect output", output, "expected", correct])


def be_admin ():
    current_user = current_username()
    clusterbank.config.set("cbank", "admins", current_user)

def not_admin ():
    clusterbank.config.remove_option("cbank", "admins")

def setup ():
    metadata.bind = create_engine("sqlite:///:memory:")
    current_user = current_username()
    upstream.users = [
        upstream.User(1, "user1"),
        upstream.User(2, "user2"),
        upstream.User(3, current_user)]
    upstream.projects = [
        upstream.Project(1, "project1"), upstream.Project(2, "project2")]
    upstream.projects[0].members.append(upstream.users[2])
    upstream.projects[1].members.append(upstream.users[0])
    upstream.projects[1].owners.append(upstream.users[2])
    upstream.resources = [
        upstream.Resource(1, "resource1"), upstream.Resource(2, "resource2")]
    clusterbank.model.upstream.use = upstream
    fake_dt = FakeDateTime(datetime(2000, 1, 1))
    clusterbank.cbank.controllers.datetime = fake_dt


def teardown ():
    upstream.users = []
    upstream.projects = []
    upstream.resources = []
    clusterbank.model.upstream.use = None
    Session.bind = None
    clusterbank.cbank.controllers.datetime = datetime


class CbankTester (object):

    def setup (self):
        metadata.create_all()
        clusterbank.config.add_section("cbank")
        for user in upstream.users:
            user_by_name(user.name)
        for project in upstream.projects:
            project_by_name(project.name)
        for resource in upstream.resources:
            resource_by_name(resource.name)
    
    def teardown (self):
        Session.remove()
        clusterbank.config.remove_section("cbank")
        metadata.drop_all()


class TestMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._report_main = controllers.report_main
        self._new_main = controllers.new_main
        controllers.report_main = FakeFunc()
        controllers.new_main = FakeFunc()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.report_main = self._report_main
        controllers.new_main = self._new_main
    
    def test_callable (self):
        assert callable(main), "main is not callable"
    
    def test_report (self):
        def test_ ():
            assert sys.argv[0] == "main report"
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_main.func = test_
        args = "report 1 2 3"
        run(main, args.split())
        assert controllers.report_main.calls
    
    def test_new (self):
        def test_ ():
            assert sys.argv[0] == "main new", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.new_main.func = test_
        args = "new 1 2 3"
        run(main, args.split())
        assert controllers.new_main.calls
    
    def test_default (self):
        def test_ ():
            assert sys.argv[0] == "main"
            assert sys.argv[1:] == args.split(), sys.argv
        controllers.report_main.func = test_
        args = "1 2 3"
        run(main, args.split())
        assert controllers.report_main.calls
    
    def test_invalid (self):
        def test_ ():
            assert sys.argv[0] == "main"
            assert sys.argv[1:] == args.split(), sys.argv
        controllers.report_main.func = test_
        args = "invalid_command 1 2 3"
        run(main, args.split())
        assert controllers.report_main.calls


class TestNewMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        be_admin()
        self._new_allocation_main = controllers.new_allocation_main
        self._new_hold_main = controllers.new_hold_main
        self._new_charge_main = controllers.new_charge_main
        self._new_refund_main = controllers.new_refund_main
        controllers.new_allocation_main = FakeFunc()
        controllers.new_hold_main = FakeFunc()
        controllers.new_charge_main = FakeFunc()
        controllers.new_refund_main = FakeFunc()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.new_allocation_main = self._new_allocation_main
        controllers.new_hold_main = self._new_hold_main
        controllers.new_charge_main = self._new_charge_main
        controllers.new_refund_main = self._new_refund_main
    
    def test_exists_and_callable (self):
        assert hasattr(controllers, "new_main"), "new_main does not exist"
        assert callable(controllers.new_main), "new_main is not callable"
    
    def test_allocation (self):
        args = "allocation 1 2 3"
        def test_ ():
            assert sys.argv[0] == "new_main allocation", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.new_allocation_main.func = test_
        run(new_main, args.split())
        assert controllers.new_allocation_main.calls
    
    def test_hold (self):
        args = "hold 1 2 3"
        def test_ ():
            assert sys.argv[0] == "new_main hold", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.new_hold_main.func = test_
        run(new_main, args.split())
        assert controllers.new_hold_main.calls
    
    def test_charge (self):
        args = "charge 1 2 3"
        def test_ ():
            assert sys.argv[0] == "new_main charge", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.new_charge_main.func = test_
        run(new_main, args.split())
        assert controllers.new_charge_main.calls
    
    def test_refund (self):
        args = "refund 1 2 3"
        def test_ ():
            assert sys.argv[0] == "new_main refund", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.new_refund_main.func = test_
        run(new_main, args.split())
        assert controllers.new_refund_main.calls
    
    def test_invalid (self):
        args = "invalid 1 2 3"
        code, stdout, stderr = run(new_main, args.split())
        assert code == UnknownCommand.exit_code, code


class TestNewAllocationMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        be_admin()
    
    def test_exists_and_callable (self):
        assert hasattr(controllers, "new_allocation_main"), \
            "new_allocation_main does not exist"
        assert callable(controllers.new_allocation_main), \
            "new_allocation_main is not callable"
    
    def test_complete (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 1000 -r resource1 -s 2008-01-01 -e 2009-01-01 -m test"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert query.count() == 1, "didn't create an allocation"
        allocation = query.one()
        assert allocation.start == datetime(2008, 1, 1), allocation.start
        assert allocation.expiration == datetime(2009, 1, 1), \
            allocation.expiration
        assert allocation.amount == 1000, allocation.amount
        assert allocation.comment == "test", allocation.comment
        assert code == 0, code
    
    def test_unknown_arguments (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = """project1 1000 -r resource1 -s 2008-01-01 \
            -e 2009-01-01 -m test asdf"""
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count()
        assert code == UnexpectedArguments.exit_code, code
    
    def test_with_defined_units (self):
        clusterbank.config.set("cbank", "unit_factor", "1/2")
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 1000 -r resource1 -s 2008-01-01 -e 2009-01-01 -m test"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert query.count() == 1, "didn't create an allocation"
        allocation = query.one()
        assert allocation.start == datetime(2008, 1, 1), allocation.start
        assert allocation.expiration == datetime(2009, 1, 1), \
            allocation.expiration
        assert allocation.amount == 2000, allocation.amount
        assert allocation.comment == "test", allocation.comment
        assert code == 0, code
    
    def test_with_bad_start (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 1000 -r resource1 -s bad_start -e 2009-01-01 -m test"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created an allocation with bad start"
        assert code != 0, code
    
    def test_with_bad_end (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 1000 -r resource1 -s 2008-01-01 -e bad_end -m test"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created an allocation with bad end"
        assert code != 0, code
    
    def test_with_bad_amount (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = """project1 bad_amount -r resource1 -s 2008-01-01 \
            -e 2009-01-01 -m test"""
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created an allocation with bad amount"
        assert code != 0, code

    def test_without_comment (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 1000 -r resource1 -s 2008-01-01 -e 2009-01-01"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert query.count() == 1, "didn't create an allocation"
        allocation = query.one()
        assert allocation.comment is None, allocation.comment
        assert code == 0, code
    
    def test_without_project (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "1000 -r resource1 -s 2008-01-01 -e 2009-01-01 -m test"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), \
            "created allocation without project: %s" % new_allocations
        assert code == UnknownProject.exit_code, code
    
    def test_without_amount (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -m test"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created allocation without amount"
        assert code == MissingArgument.exit_code, code
    
    def test_without_start (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 1000 -r resource1 -e 2009-01-01 -m test"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert query.count() == 1, "didn't create an allocation"
        allocation = query.one()
        assert allocation.start == datetime(2000, 1, 1), allocation.start
        assert code == 0, code
    
    def test_without_expiration (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 1000 -r resource1 -s 2000-01-01 -m test"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert query.count() == 1, "didn't create an allocation"
        allocation = query.one()
        now = datetime(2000, 1, 1)
        assert allocation.start == datetime(2000, 1, 1), allocation.start
        assert allocation.expiration == datetime(2001, 1, 1), \
            allocation.expiration
        assert code == 0, code

    def test_without_resource (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 1000 -s 2008-01-01 -e 2009-01-01 -m test"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), \
            "created allocation without resource: %s" % new_allocations
        assert code == MissingResource.exit_code, code
    
    def test_with_configured_resource (self):
        clusterbank.config.set("cbank", "resource", "resource1")
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 1000 -s 2008-01-01 -e 2009-01-01 -m test"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        assert query.count() == 1, "didn't create an allocation"
        allocation = query.one()
        assert allocation.resource is resource
        assert code == 0, code

    def test_non_admin (self):
        clusterbank.config.set("cbank", "admins", "")
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 1000 -r resource1 -s 2008-01-01 -e 2009-01-01 -m test"
        code, stdout, stderr = run(
            controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), \
            "created allocation when not admin: %s" % new_allocations
        assert code == NotPermitted.exit_code, code


class TestNewChargeMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        be_admin()
    
    def test_exists_and_callable (self):
        assert hasattr(controllers, "new_charge_main"), \
            "new_charge_main does not exist"
        assert callable(controllers.new_charge_main), \
            "new_charge_main is not callable"
    
    def test_complete (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        user = user_by_name("user1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(
            project=project, resource=resource, amount=1000,
            start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "project1 100 -r resource1 -m test -u user1"
        code, stdout, stderr = run(new_charge_main, args.split())
        assert code == 0
        assert charges.count() == 1, "didn't create a charge"
        charge = charges.one()
        assert charge.allocation is allocation, \
            "incorrect allocation: %r" % charge.allocation
        assert charge.amount == 100, \
            "incorrect charge amount: %i" % charge.amount
        assert charge.comment == "test", \
            "incorrect comment: %s" % charge.comment
        assert charge.user is user, \
            "incorrect user on charge: %s" % charge.user
    
    def test_unknown_arguments (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        user = user_by_name("user1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(
            project=project, resource=resource, amount=1000,
            start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "project1 100 -r resource1 -m test -u user1 asdf"
        code, stdout, stderr = run(new_charge_main, args.split())
        assert not charges.count()
        assert code == UnexpectedArguments.exit_code, code
    
    def test_with_defined_units (self):
        clusterbank.config.set("cbank", "unit_factor", "1/2")
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        user = user_by_name("user1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(
            project=project, resource=resource, amount=1000,
            start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "project1 100 -r resource1 -m test -u user1"
        code, stdout, stderr = run(new_charge_main, args.split())
        assert code == 0
        assert charges.count() == 1, "didn't create a charge"
        charge = charges.one()
        assert charge.allocation is allocation, \
            "incorrect allocation: %r" % charge.allocation
        assert charge.amount == 200, \
            "incorrect charge amount: %i" % charge.amount
        assert charge.comment == "test", \
            "incorrect comment: %s" % charge.comment
        assert charge.user is user, \
            "incorrect user on charge: %s" % charge.user
    
    def test_without_resource (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        user = user_by_name("user1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(
            project=project, resource=resource, amount=1000,
            start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "project1 100 -m test -u user1"
        code, stdout, stderr = run(new_charge_main, args.split())
        assert code == MissingResource.exit_code, code
        assert not charges.count(), "created a charge"
    
    def test_with_configured_resource (self):
        clusterbank.config.set("cbank", "resource", "resource1")
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        user = user_by_name("user1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(
            project=project, resource=resource, amount=1000,
            start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "project1 100 -m test -u user1"
        code, stdout, stderr = run(new_charge_main, args.split())
        assert code == 0, code
        assert charges.count(), "didn't create a charge"
        charge = charges.one()
        assert charge.allocation.resource is resource

    def test_without_project (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        user = user_by_name("user1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(
            project=project, resource=resource, amount=1000,
            start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "100 -r resource1 -m test -u user1"
        code, stdout, stderr = run(new_charge_main, args.split())
        assert code == UnknownProject.exit_code, code
        assert not charges.count(), "created a charge"
    
    def test_without_amount (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        user = user_by_name("user1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(
            project=project, resource=resource, amount=1000,
            start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "project1 -r resource1 -m test -u user1"
        code, stdout, stderr = run(new_charge_main, args.split())
        assert code == MissingArgument.exit_code, code
        assert not charges.count(), "created a charge"
    
    def test_with_negative_amount (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        user = user_by_name("user1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(
            project=project, resource=resource, amount=1000,
            start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "project1 '-100' -r resource1 -m test -u user1"
        code, stdout, stderr = run(new_charge_main, args.split())
        Session.remove()
        assert not charges.count(), \
            "created a charge with negative amount: %s" % [
                (charge, charge.amount) for charge in charges]
        assert code == ValueError_.exit_code, code
    
    def test_without_comment (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        user = user_by_name("user1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(
            project=project, resource=resource, amount=1000,
            start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "project1 100 -r resource1 -u user1"
        code, stdout, stderr = run(new_charge_main, args.split())
        assert code == 0
        assert charges.count() == 1, "didn't create a charge"
        charge = charges.one()
        assert charge.allocation is allocation, \
            "incorrect allocation: %r" % charge.allocation
        assert charge.amount == 100, \
            "incorrect charge amount: %i" % charge.amount
        assert charge.comment is None, \
            "incorrect comment: %s" % charge.comment
        assert charge.user is user, \
            "incorrect user on charge: %s" % charge.user
    
    def test_without_user (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(
            project=project, resource=resource, amount=1000,
            start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "project1 100 -r resource1 -m test"
        code, stdout, stderr = run(new_charge_main, args.split())
        assert code == 0, 0
        assert charges.count() == 1, "didn't create a charge"
        charge = charges.one()
        assert charge.allocation is allocation, \
            "incorrect allocation: %r" % charge.allocation
        assert charge.amount == 100, \
            "incorrect charge amount: %i" % charge.amount
        assert charge.comment == "test", \
            "incorrect comment: %s" % charge.comment
        assert charge.user is user_by_name(current_username()), \
            "incorrect user on charge: %s" % charge.user
    
    def test_non_admin (self):
        clusterbank.config.set("cbank", "admins", "")
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(project=project, resource=resource,
            amount=1000, start=now-timedelta(days=1),
            expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "project1 100 -r resource1 -m test"
        code, stdout, stderr = run(new_charge_main, args.split())
        Session.remove()
        assert not charges.count(), "created a charge without admin privileges"
        assert code == NotPermitted.exit_code, code


class TestNewRefundMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        be_admin()
    
    def test_exists_and_callable (self):
        assert hasattr(controllers, "new_refund_main"), \
            "new_refund_main does not exist"
        assert callable(controllers.new_refund_main), \
            "new_refund_main is not callable"
    
    def test_complete (self):
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource,
            amount=1000, start=now-timedelta(days=1),
            expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "%s 50 -m test" % charge.id
        code, stdout, stderr = run(new_refund_main, args.split())
        assert code == 0, code
        assert refunds.count() == 1, "didn't create a refund"
        refund = refunds.one()
        assert refund.charge is charge, refund.charge
        assert refund.amount == 50, refund.amount
        assert refund.comment == "test", refund.comment
    
    def test_unknown_arguments (self):
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource,
            amount=1000, start=now-timedelta(days=1),
            expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "%s 50 -m test asdf" % charge.id
        code, stdout, stderr = run(new_refund_main, args.split())
        assert not refunds.count()
        assert code == UnexpectedArguments.exit_code, code
    
    def test_with_defined_units (self):
        clusterbank.config.set("cbank", "unit_factor", "1/2")
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource,
            amount=1000, start=now-timedelta(days=1),
            expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "%s 50 -m test" % charge.id
        code, stdout, stderr = run(new_refund_main, args.split())
        assert code == 0, code
        assert refunds.count() == 1, "didn't create a refund"
        refund = refunds.one()
        assert refund.charge is charge, refund.charge
        assert refund.amount == 100, refund.amount
        assert refund.comment == "test", refund.comment
    
    def test_without_comment (self):
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource,
            amount=1000, start=now-timedelta(days=1),
            expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "%s 50" % charge.id
        code, stdout, stderr = run(new_refund_main, args.split())
        assert code == 0, code
        assert refunds.count() == 1, "didn't create a refund"
        refund = refunds.one()
        assert refund.charge is charge, refund.charge
        assert refund.amount == 50, refund.amount
        assert refund.comment is None, refund.comment
    
    def test_without_charge (self):
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource,
            amount=1000, start=now-timedelta(days=1),
            expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "50 -m test"
        code, stdout, stderr = run(new_refund_main, args.split())
        Session.remove()
        assert not refunds.count(), "created refund without charge"
        assert code in (
            MissingArgument.exit_code,
            UnknownCharge.exit_code), code
    
    def test_without_amount (self):
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource,
            amount=1000, start=now-timedelta(days=1),
            expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "%s -m test" % charge.id
        code, stdout, stderr = run(new_refund_main, args.split())
        Session.remove()
        assert refunds.count() == 1, "incorrect refund count: %r" %[
            (refund, refund.amount) for refund in refunds]
        refund = refunds.one()
        assert refund.amount == 100
        assert code == 0, code
    
    def test_without_amount_with_existing_refund (self):
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource,
            amount=1000, start=now-timedelta(days=1),
            expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        refund = Refund(charge, 25)
        Session.save(allocation)
        Session.save(charge)
        Session.save(refund)
        Session.commit()
        args = "%s -m test" % charge.id
        code, stdout, stderr = run(new_refund_main, args.split())
        assert code == 0, code
        Session.remove()
        assert refunds.count() == 2, "incorrect refund count: %r" % [
            (refund, refund.amount) for refund in refunds]
        assert sum(refund.amount for refund in refunds) == 100
    
    def test_non_admin (self):
        clusterbank.config.set("cbank", "admins", "")
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource,
            amount=1000, start=now-timedelta(days=1),
            expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "%s 50 -m test" % charge.id
        code, stdout, stderr = run(new_refund_main, args.split())
        Session.remove()
        assert not refunds.count(), "created a refund when not an admin"
        assert code == NotPermitted.exit_code, code


class TestReportMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._report_users_main = \
            controllers.report_users_main
        self._report_projects_main = \
            controllers.report_projects_main
        self._report_allocations_main = \
            controllers.report_allocations_main
        self._report_holds_main = \
            controllers.report_holds_main
        self._report_charges_main = \
            controllers.report_charges_main
        controllers.report_users_main = FakeFunc()
        controllers.report_projects_main = FakeFunc()
        controllers.report_allocations_main = FakeFunc()
        controllers.report_holds_main = FakeFunc()
        controllers.report_charges_main = FakeFunc()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.report_users_main = \
            self._report_users_main
        controllers.report_projects_main = \
            self._report_projects_main
        controllers.report_allocations_main = \
            self._report_allocations_main
        controllers.report_holds_main = \
            self._report_holds_main
        controllers.report_charges_main = \
            self._report_charges_main
    
    def test_exists_and_callable (self):
        assert hasattr(controllers, "report_main"), \
            "report_main does not exist"
        assert callable(controllers.report_main), \
            "report_main is not callable"
    
    def test_users (self):
        def test_ ():
            assert sys.argv[0] == "report_main users", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_users_main.func = test_
        args = "users 1 2 3"
        run(report_main, args.split())
        assert controllers.report_users_main.calls
    
    def test_projects (self):
        def test_ ():
            assert sys.argv[0] == "report_main projects", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_projects_main.func = test_
        args = "projects 1 2 3"
        run(report_main, args.split())
        assert controllers.report_projects_main.calls
    
    def test_allocations (self):
        def test_ ():
            assert sys.argv[0] == "report_main allocations", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_allocations_main.func = test_
        args = "allocations 1 2 3"
        run(report_main, args.split())
        assert controllers.report_allocations_main.calls
    
    def test_holds (self):
        def test_ ():
            assert sys.argv[0] == "report_main holds", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_holds_main.func = test_
        args = "holds 1 2 3"
        run(report_main, args.split())
        assert controllers.report_holds_main.calls
    
    def test_charges (self):
        def test_ ():
            assert sys.argv[0] == "report_main charges", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_charges_main.func = test_
        args = "charges 1 2 3"
        run(report_main, args.split())
        assert controllers.report_charges_main.calls
    
    def test_default (self):
        def test_ ():
            assert sys.argv[0] == "report_main", sys.argv
            assert sys.argv[1:] == args.split(), sys.argv
        controllers.report_projects_main.func = test_
        args = "1 2 3"
        run(report_main, args.split())
        assert controllers.report_projects_main.calls
    
    def test_invalid (self):
        def test_ ():
            assert sys.argv[0] == "report_main", sys.argv
            assert sys.argv[1:] == args.split(), sys.argv
        controllers.report_projects_main.func = test_
        args = "invalid 1 2 3"
        run(report_main, args.split())
        assert controllers.report_projects_main.calls


class TestUsersReport (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._print_users_report = controllers.print_users_report
        controllers.print_users_report = FakeFunc()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.print_users_report = self._print_users_report
    
    def test_default (self):
        """Current user's charges filtered by user's projects."""
        user = user_by_name(current_username())
        projects = user_projects(user)
        code, stdout, stderr = run(report_users_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        assert_equal(set(args[0]), set([user]))
        assert_equal(set(kwargs['projects']), set(projects))
    
    def test_other_users (self):
        """Non-admin cannot specify other users."""
        code, stdout, stderr = run(report_users_main, "-u user1".split())
        assert_equal(code, NotPermitted.exit_code)
        assert not controllers.print_users_report.calls
    
    def test_other_projects (self):
        """anyone can specify any project filters"""
        code, stdout, stderr = run(
            report_users_main, "-p project1 -p project2".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        projects = [project_by_name(project)
            for project in ["project1", "project2"]]
        assert_equal(set(kwargs['projects']), set(projects))
    
    def test_owner (self):
        users = project_members(project_by_name("project2"))
        code, stdout, stderr = run(report_users_main, "-p project2".split())
        args, kwargs = controllers.print_users_report.calls[0]
        assert_equal(code, 0)
        assert_equal(set(args[0]), set(users))
 

class TestUsersReport_Admin (TestUsersReport):
    
    def setup (self):
        TestUsersReport.setup(self)
        be_admin()
    
    def test_default (self):
        """All users, no filters."""
        users = Session().query(User).all()
        code, stdout, stderr = run(report_users_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        assert_equal(set(args[0]), set(users))
        assert_equal(kwargs['projects'], [])
    
    def test_other_users (self):
        """admin can specify other users."""
        code, stdout, stderr = run(
            report_users_main, "-u user1 -u user2".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        users = [user_by_name(user) for user in ["user1", "user2"]]
        assert_equal(set(args[0]), set(users))
        assert_equal(kwargs['projects'], [])
 
