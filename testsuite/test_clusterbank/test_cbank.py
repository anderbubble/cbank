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
    resource_by_name, Session, Allocation, Hold, Charge, Refund
from clusterbank.model.database import metadata
import clusterbank.upstreams.default as upstream
import clusterbank.cbank.controllers as controllers
import clusterbank.cbank.exceptions as exceptions


def get_current_username ():
    return pwd.getpwuid(os.getuid())[0]


class FakeDateTime (object):
    
    def __init__ (self, now):
        self._now = now
    
    def __call__ (self, *args):
        return datetime(*args)
    
    def now (self):
        return self._now


def setup ():
    metadata.bind = create_engine("sqlite:///:memory:")
    upstream.projects = [
        upstream.Project(1, "project1"), upstream.Project(2, "project2")]
    upstream.resources = [
        upstream.Resource(1, "resource1"), upstream.Resource(2, "resource2")]
    current_user = get_current_username()
    upstream.users = [
        upstream.User(1, "user1"),
        upstream.User(2, "user2"),
        upstream.User(3, current_user)]
    clusterbank.model.upstream.use = upstream
    fake_dt = FakeDateTime(datetime(2000, 1, 1))
    clusterbank.cbank.views.datetime = fake_dt
    clusterbank.cbank.controllers.datetime = fake_dt


def teardown ():
    upstream.users = []
    upstream.projects = []
    upstream.resources = []
    clusterbank.model.upstream.use = None
    Session.bind = None
    clusterbank.cbank.views.datetime = datetime
    clusterbank.cbank.controllers.datetime = datetime


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


class CbankTester (object):

    def setup (self):
        metadata.create_all()
        current_user = get_current_username()
        clusterbank.config.add_section("cbank")
        clusterbank.config.set("cbank", "admins", current_user)
        self.fake_called = False
        for user in upstream.users:
            user_by_name(user.name)
        for project in upstream.projects:
            project_by_name(project.name)
        for resource in upstream.resources:
            resource_by_name(resource.name)
    
    def teardown (self):
        metadata.drop_all()
        Session.remove()
        clusterbank.config.remove_section("cbank")


class TestMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._report_main = controllers.report_main
        self._new_main = controllers.new_main
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.report_main = self._report_main
        controllers.new_main = self._new_main
    
    def test_exists_and_callable (self):
        assert hasattr(controllers, "main"), "main does not exist"
        assert callable(controllers.main), "main is not callable"
    
    def test_report (self):
        args = "report 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "main report"
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_main = fake
        run(controllers.main, args.split())
        assert self.fake_called
    
    def test_new (self):
        args = "new 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "main new", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.new_main = fake
        run(controllers.main, args.split())
        assert self.fake_called
    
    def test_default (self):
        args = "1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "main"
            assert sys.argv[1:] == args.split(), sys.argv
        controllers.report_main = fake
        run(controllers.main, args.split())
        assert self.fake_called
    
    def test_invalid (self):
        args = "invalid_command 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "main"
            assert sys.argv[1:] == args.split(), sys.argv
        controllers.report_main = fake
        run(controllers.main, args.split())
        assert self.fake_called


class TestNewMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._new_allocation_main = \
            controllers.new_allocation_main
        self._new_charge_main = \
            controllers.new_charge_main
        self._new_refund_main = \
            controllers.new_refund_main
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.new_allocation_main = \
            self._new_allocation_main
        controllers.new_charge_main = \
            self._new_charge_main
        controllers.new_refund_main = \
            self._new_refund_main
    
    def test_exists_and_callable (self):
        assert hasattr(controllers, "new_main"), "new_main does not exist"
        assert callable(controllers.new_main), "new_main is not callable"
    
    def test_allocation (self):
        args = "allocation 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "new_main allocation", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.new_allocation_main = fake
        run(controllers.new_main, args.split())
        assert self.fake_called
    
    def test_charge (self):
        args = "charge 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "new_main charge", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.new_charge_main = fake
        run(controllers.new_main, args.split())
        assert self.fake_called
    
    def test_refund (self):
        args = "refund 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "new_main refund", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.new_refund_main = fake
        run(controllers.new_main, args.split())
        assert self.fake_called
        
    
    def test_invalid (self):
        args = "invalid 1 2 3"
        code, stdout, stderr = run(controllers.new_main, args.split())
        assert code == exceptions.UnknownCommand.exit_code, code


class TestNewAllocationMain (CbankTester):
    
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
        print code
        print stdout.read()
        print stderr.read()
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
        assert code == exceptions.UnexpectedArguments.exit_code, code
    
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
        assert code == exceptions.UnknownProject.exit_code, code
    
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
        assert code == exceptions.MissingArgument.exit_code, code
    
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
        assert code == exceptions.MissingResource.exit_code, code
    
    def test_with_configured_resource (self):
        clusterbank.config.set("cbank", "resource", "resource1")
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(
            project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "project1 1000 -s 2008-01-01 -e 2009-01-01 -m test"
        code, stdout, stderr = run(controllers.new_allocation_main, args.split())
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
        assert code == exceptions.NotPermitted.exit_code, code


class TestNewChargeMain (CbankTester):
    
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
        code, stdout, stderr = run(controllers.new_charge_main, args.split())
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
        code, stdout, stderr = run(controllers.new_charge_main, args.split())
        assert not charges.count()
        assert code == exceptions.UnexpectedArguments.exit_code, code
    
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
        code, stdout, stderr = run(controllers.new_charge_main, args.split())
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
        code, stdout, stderr = run(controllers.new_charge_main, args.split())
        assert code == exceptions.MissingResource.exit_code, code
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
        code, stdout, stderr = run(controllers.new_charge_main, args.split())
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
        code, stdout, stderr = run(controllers.new_charge_main, args.split())
        assert code == exceptions.UnknownProject.exit_code, code
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
        code, stdout, stderr = run(controllers.new_charge_main, args.split())
        assert code == exceptions.MissingArgument.exit_code, code
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
        code, stdout, stderr = run(controllers.new_charge_main, args.split())
        Session.remove()
        assert not charges.count(), \
            "created a charge with negative amount: %s" % [
                (charge, charge.amount) for charge in charges]
        assert code == exceptions.ValueError.exit_code, code
    
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
        code, stdout, stderr = run(controllers.new_charge_main, args.split())
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
        code, stdout, stderr = run(controllers.new_charge_main, args.split())
        assert code == 0, 0
        assert charges.count() == 1, "didn't create a charge"
        charge = charges.one()
        assert charge.allocation is allocation, \
            "incorrect allocation: %r" % charge.allocation
        assert charge.amount == 100, \
            "incorrect charge amount: %i" % charge.amount
        assert charge.comment == "test", \
            "incorrect comment: %s" % charge.comment
        assert charge.user is user_by_name(get_current_username()), \
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
        code, stdout, stderr = run(controllers.new_charge_main, args.split())
        Session.remove()
        assert not charges.count(), "created a charge without admin privileges"
        assert code == exceptions.NotPermitted.exit_code, code


class TestNewRefundMain (CbankTester):
    
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
        code, stdout, stderr = run(controllers.new_refund_main, args.split())
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
        code, stdout, stderr = run(controllers.new_refund_main, args.split())
        assert not refunds.count()
        assert code == exceptions.UnexpectedArguments.exit_code, code
    
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
        code, stdout, stderr = run(controllers.new_refund_main, args.split())
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
        code, stdout, stderr = run(controllers.new_refund_main, args.split())
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
        code, stdout, stderr = run(controllers.new_refund_main, args.split())
        Session.remove()
        assert not refunds.count(), "created refund without charge"
        assert code in (
            exceptions.MissingArgument.exit_code,
            exceptions.UnknownCharge.exit_code), code
    
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
        code, stdout, stderr = run(controllers.new_refund_main, args.split())
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
        code, stdout, stderr = run(controllers.new_refund_main, args.split())
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
        code, stdout, stderr = run(controllers.new_refund_main, args.split())
        Session.remove()
        assert not refunds.count(), "created a refund when not an admin"
        assert code == exceptions.NotPermitted.exit_code, code


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
    
    def test_users (self):
        args = "users 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main users", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_users_main = fake
        run(controllers.report_main, args.split())
        assert self.fake_called
    
    def test_projects (self):
        args = "projects 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main projects", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_projects_main = fake
        run(controllers.report_main, args.split())
        assert self.fake_called
    
    def test_allocations (self):
        args = "allocations 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main allocations", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_allocations_main = fake
        run(controllers.report_main, args.split())
        assert self.fake_called
    
    def test_holds (self):
        args = "holds 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main holds", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_holds_main = fake
        run(controllers.report_main, args.split())
        assert self.fake_called
    
    def test_charges (self):
        args = "charges 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main charges", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_charges_main = fake
        run(controllers.report_main, args.split())
        assert self.fake_called
    
    def test_default (self):
        args = "1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main", sys.argv
            assert sys.argv[1:] == args.split(), sys.argv
        controllers.report_projects_main = fake
        run(controllers.report_main, args.split())
        assert self.fake_called
    
    def test_invalid (self):
        args = "invalid 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main", sys.argv
            assert sys.argv[1:] == args.split(), sys.argv
        controllers.report_projects_main = fake
        run(controllers.report_main, args.split())
        assert self.fake_called
    
    def test_exists_and_callable (self):
        assert hasattr(controllers, "report_main"), \
            "report_main does not exist"
        assert callable(controllers.report_main), \
            "report_main is not callable"
    
    def test_admin_reports_complete (self):
        self._run_all_reports()
    
    def test_non_admin_reports_complete (self):
        clusterbank.config.set("cbank", "admins", "")
        self._run_all_reports()
    
    def _run_all_reports (self):
        for report in ("users", "projects", "allocations", "holds", "charges"):
            code, stdout, stderr = run(controllers.report_main, [report])
            assert code == 0, report


class TestReportUsers (CbankTester):
    
    def test_blank (self):
        code, stdout, stderr = run(controllers.report_users_main, [
            "-p", "project1", "-p", "project2",
            "-u", "user1", "-u", "user2",
            "-r", "resource1", "-r", "resource2"])
        correct = dedent("""\
            Name        Charges         Charged
            ---------- -------- ---------------
            user1             0             0.0
            user2             0             0.0
                       -------- ---------------
                              0             0.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_charges (self):
        start = datetime.now()
        end = start + timedelta(weeks=1)
        a1 = Allocation(project_by_name("project1"),
            resource_by_name("resource1"), 100, start, end)
        a2 = Allocation(project_by_name("project2"),
            resource_by_name("resource1"), 100, start, end)
        Charge(a1, 10).user = user_by_name("user1")
        Charge(a1, 7).user = user_by_name("user2")
        Charge(a2, 3).user = user_by_name("user2")
        Charge(a2, 5).user = user_by_name("user2")
        code, stdout, sterr = run(controllers.report_users_main, [
            "-p", "project1", "-p", "project2",
            "-u", "user1", "-u", "user2",
            "-r", "resource1", "-r", "resource2"])
        correct = dedent("""\
            Name        Charges         Charged
            ---------- -------- ---------------
            user1             1            10.0
            user2             3            15.0
                       -------- ---------------
                              4            25.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_refunds (self):
        start = datetime.now()
        end = start + timedelta(weeks=1)
        a1 = Allocation(project_by_name("project1"),
            resource_by_name("resource1"), 100, start, end)
        a2 = Allocation(project_by_name("project2"),
            resource_by_name("resource1"), 100, start, end)
        c1 = Charge(a1, 10)
        c1.user = user_by_name("user1")
        c2 = Charge(a1, 7)
        c2.user = user_by_name("user2")
        Charge(a2, 3).user = user_by_name("user2")
        c4 = Charge(a2, 5)
        c4.user = user_by_name("user2")
        Refund(c1, 9)
        Refund(c2, 3)
        Refund(c2, 4)
        Refund(c4, 3)
        code, stdout, sterr = run(controllers.report_users_main, [
            "-p", "project1", "-p", "project2",
            "-u", "user1", "-u", "user2",
            "-r", "resource1", "-r", "resource2"])
        correct = dedent("""\
            Name        Charges         Charged
            ---------- -------- ---------------
            user1             1             1.0
            user2             3             5.0
                       -------- ---------------
                              4             6.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)


class TestReportProjects (CbankTester):
    
    def run_report (self):
        return run(controllers.report_projects_main, [
            "-p", "project1", "-p", "project2",
            "-u", "user1", "-u", "user2",
            "-r", "resource1", "-r", "resource2"])
    
    def test_blank (self):
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            Name            Charges         Charged       Available
            --------------- ------- --------------- ---------------
            project1              0             0.0             0.0
            project2              0             0.0             0.0
                            ------- --------------- ---------------
                                  0             0.0             0.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_allocations (self):
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        Allocation(project1, resource1, 10, start, end)
        Allocation(project1, resource1, 20, start, end)
        Allocation(project2, resource1, 30, start, end)
        Allocation(project2, resource2, 35, start, end)
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            Name            Charges         Charged       Available
            --------------- ------- --------------- ---------------
            project1              0             0.0            30.0
            project2              0             0.0            65.0
                            ------- --------------- ---------------
                                  0             0.0            95.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_holds (self):
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        Hold(a1, 10)
        a2 = Allocation(project1, resource1, 20, start, end)
        Hold(a2, 15)
        Hold(a2, 5)
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Hold(a4, 9)
        Hold(a4, 8)
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            Name            Charges         Charged       Available
            --------------- ------- --------------- ---------------
            project1              0             0.0             0.0
            project2              0             0.0            48.0
                            ------- --------------- ---------------
                                  0             0.0            48.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_charges (self):
        user1 = user_by_name("user1")
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        Charge(a1, 10).user = user1
        a2 = Allocation(project1, resource1, 20, start, end)
        Charge(a2, 15).user = user1
        Charge(a2, 5).user = user1
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Charge(a4, 9).user = user1
        Charge(a4, 8).user = user1
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            Name            Charges         Charged       Available
            --------------- ------- --------------- ---------------
            project1              3            30.0             0.0
            project2              2            17.0            48.0
                            ------- --------------- ---------------
                                  5            47.0            48.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_refunds (self):
        user1 = user_by_name("user1")
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        c1 = Charge(a1, 10)
        c1.user = user1
        Refund(c1, 4)
        a2 = Allocation(project1, resource1, 20, start, end)
        c2 = Charge(a2, 15)
        c2.user = user1
        Refund(c2, 3)
        Refund(c2, 5)
        Charge(a2, 5).user = user1
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Charge(a4, 9).user = user1
        c5 = Charge(a4, 8)
        c5.user = user1
        Refund(c5, 8)
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            Name            Charges         Charged       Available
            --------------- ------- --------------- ---------------
            project1              3            18.0            12.0
            project2              2             9.0            56.0
                            ------- --------------- ---------------
                                  5            27.0            68.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)


class TestReportAllocations (CbankTester):
    
    def run_report (self):
        return run(controllers.report_allocations_main, [
            "-p", "project1", "-p", "project2",
            "-u", "user1", "-u", "user2",
            "-r", "resource1", "-r", "resource2"])
    
    def test_blank (self):
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
                                                     ------- ------------- -------------
                                                           0           0.0           0.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_allocations (self):
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        Allocation(project1, resource1, 10, start, end)
        Allocation(project1, resource1, 20, start, end)
        Allocation(project2, resource1, 30, start, end)
        Allocation(project2, resource2, 35, start, end)
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
            1    2000-01-08 resource1 project1              0           0.0          10.0
            2    2000-01-08 resource1 project1              0           0.0          20.0
            3    2000-01-08 resource1 project2              0           0.0          30.0
            4    2000-01-08 resource2 project2              0           0.0          35.0
                                                     ------- ------------- -------------
                                                           0           0.0          95.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_holds (self):
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        Hold(a1, 10)
        a2 = Allocation(project1, resource1, 20, start, end)
        Hold(a2, 15)
        Hold(a2, 5)
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Hold(a4, 9)
        Hold(a4, 8)
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
            1    2000-01-08 resource1 project1              0           0.0           0.0
            2    2000-01-08 resource1 project1              0           0.0           0.0
            3    2000-01-08 resource1 project2              0           0.0          30.0
            4    2000-01-08 resource2 project2              0           0.0          18.0
                                                     ------- ------------- -------------
                                                           0           0.0          48.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_charges (self):
        user1 = user_by_name("user1")
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        Charge(a1, 10).user = user1
        a2 = Allocation(project1, resource1, 20, start, end)
        Charge(a2, 15).user = user1
        Charge(a2, 5).user = user1
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Charge(a4, 9).user = user1
        Charge(a4, 8).user = user1
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
            1    2000-01-08 resource1 project1              1          10.0           0.0
            2    2000-01-08 resource1 project1              2          20.0           0.0
            3    2000-01-08 resource1 project2              0           0.0          30.0
            4    2000-01-08 resource2 project2              2          17.0          18.0
                                                     ------- ------------- -------------
                                                           5          47.0          48.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_refunds (self):
        user1 = user_by_name("user1")
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        c1 = Charge(a1, 10)
        c1.user = user1
        Refund(c1, 4)
        a2 = Allocation(project1, resource1, 20, start, end)
        c2 = Charge(a2, 15)
        c2.user = user1
        Refund(c2, 3)
        Refund(c2, 5)
        Charge(a2, 5).user = user1
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        Charge(a4, 9).user = user1
        c5 = Charge(a4, 8)
        c5.user = user1
        Refund(c5, 8)
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            #    Expiration Resource Project         Charges       Charged     Available
            ---- ---------- -------- --------------- ------- ------------- -------------
            1    2000-01-08 resource1 project1              1           6.0           4.0
            2    2000-01-08 resource1 project1              2          12.0           8.0
            3    2000-01-08 resource1 project2              0           0.0          30.0
            4    2000-01-08 resource2 project2              2           9.0          26.0
                                                     ------- ------------- -------------
                                                           5          27.0          68.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

class TestReportHolds (CbankTester):
    
    def run_report (self):
        return run(controllers.report_holds_main, [
            "-p", "project1", "-p", "project2",
            "-u", "user1", "-u", "user2",
            "-r", "resource1", "-r", "resource2"])
    
    def test_blank (self):
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            #      Date       Resource Project         User              Held
            ------ ---------- -------- --------------- -------- -------------
                                                                -------------
                                                                          0.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_holds (self):
        user1, user2 = [user_by_name(u)
            for u in ("user1", "user2")]
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        h1 = Hold(a1, 10)
        a2 = Allocation(project1, resource1, 20, start, end)
        h2 = Hold(a2, 15)
        h3 = Hold(a2, 5)
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        h4 = Hold(a4, 9)
        h5 = Hold(a4, 8)
        for hold in (h1, h2, h3):
            hold.user = user1
        for hold in (h4, h5):
            hold.user = user2
        for hold in (h1, h2, h3, h4, h5):
            hold.datetime = datetime(2000, 1, 1)
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            #      Date       Resource Project         User              Held
            ------ ---------- -------- --------------- -------- -------------
            1      2000-01-01 resource1 project1        user1             10.0
            2      2000-01-01 resource1 project1        user1             15.0
            3      2000-01-01 resource1 project1        user1              5.0
            4      2000-01-01 resource2 project2        user2              9.0
            5      2000-01-01 resource2 project2        user2              8.0
                                                                -------------
                                                                         47.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_inactive_holds (self):
        user1, user2 = [user_by_name(u)
            for u in ("user1", "user2")]
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        h1 = Hold(a1, 10)
        a2 = Allocation(project1, resource1, 20, start, end)
        h2 = Hold(a2, 15)
        h3 = Hold(a2, 5)
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        h4 = Hold(a4, 9)
        h5 = Hold(a4, 8)
        for hold in (h1, h2, h3):
            hold.user = user1
        for hold in (h4, h5):
            hold.user = user2
        for hold in (h1, h2, h3, h4, h5):
            hold.datetime = datetime(2000, 1, 1)
        for hold in (h2, h4, h5):
            hold.active = False
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            #      Date       Resource Project         User              Held
            ------ ---------- -------- --------------- -------- -------------
            1      2000-01-01 resource1 project1        user1             10.0
            3      2000-01-01 resource1 project1        user1              5.0
                                                                -------------
                                                                         15.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)


class TestReportCharges (CbankTester):
    
    def run_report (self):
        return run(controllers.report_charges_main, [
            "-p", "project1", "-p", "project2",
            "-u", "user1", "-u", "user2",
            "-r", "resource1", "-r", "resource2"])
    
    def test_blank (self):
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            #      Date       Resource Project         User           Charged
            ------ ---------- -------- --------------- -------- -------------
                                                                -------------
                                                                          0.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

    def test_charges (self):
        user1, user2 = [user_by_name(u)
            for u in ("user1", "user2")]
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        c1 = Charge(a1, 10)
        a2 = Allocation(project1, resource1, 20, start, end)
        c2 = Charge(a2, 15)
        c3 = Charge(a2, 5)
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        c4 = Charge(a4, 9)
        c5 = Charge(a4, 8)
        for charge in (c1, c2, c3):
            charge.user = user1
        for charge in (c4, c5):
            charge.user = user2
        for charge in (c1, c2, c3, c4, c5):
            charge.datetime = datetime(2000, 1, 1)
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            #      Date       Resource Project         User           Charged
            ------ ---------- -------- --------------- -------- -------------
            #1     2000-01-01 resource1 project1        user1             10.0
            #2     2000-01-01 resource1 project1        user1             15.0
            #3     2000-01-01 resource1 project1        user1              5.0
            #4     2000-01-01 resource2 project2        user2              9.0
            #5     2000-01-01 resource2 project2        user2              8.0
                                                                -------------
                                                                         47.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)
    
    def test_refunds (self):
        user1, user2 = [user_by_name(u)
            for u in ("user1", "user2")]
        project1, project2 = [project_by_name(p)
            for p in ("project1", "project2")]
        resource1, resource2 = [resource_by_name(r)
            for r in ("resource1", "resource2")]
        start = datetime(2000, 1, 1)
        end = start + timedelta(weeks=1)
        a1 = Allocation(project1, resource1, 10, start, end)
        c1 = Charge(a1, 10)
        Refund(c1, 4)
        a2 = Allocation(project1, resource1, 20, start, end)
        c2 = Charge(a2, 15)
        Refund(c2, 3)
        Refund(c2, 5)
        c3 = Charge(a2, 5)
        Allocation(project2, resource1, 30, start, end)
        a4 = Allocation(project2, resource2, 35, start, end)
        c4 = Charge(a4, 9)
        c5 = Charge(a4, 8)
        Refund(c5, 8)
        for charge in (c1, c2, c3):
            charge.user = user1
        for charge in (c4, c5):
            charge.user = user2
        for charge in (c1, c2, c3, c4, c5):
            charge.datetime = datetime(2000, 1, 1)
        code, stdout, stderr = self.run_report()
        correct = dedent("""\
            #      Date       Resource Project         User           Charged
            ------ ---------- -------- --------------- -------- -------------
            #1     2000-01-01 resource1 project1        user1              6.0
            #2     2000-01-01 resource1 project1        user1              7.0
            #3     2000-01-01 resource1 project1        user1              5.0
            #4     2000-01-01 resource2 project2        user2              9.0
            #5     2000-01-01 resource2 project2        user2              0.0
                                                                -------------
                                                                         27.0
            Units are undefined.
            """)
        assert_eq_output(stdout.getvalue(), correct)

