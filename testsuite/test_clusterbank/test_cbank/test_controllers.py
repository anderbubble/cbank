import sys
import pwd
import os
from datetime import datetime, timedelta
from StringIO import StringIO
from textwrap import dedent

from sqlalchemy import create_engine

import clusterbank
from clusterbank.model import upstream, metadata, User, Project, Allocation, \
    Hold, Job, Charge, Refund
from clusterbank.controllers import user, project, resource, user_by_name, \
    project_by_name, resource_by_name, Session
import clusterbank.upstreams.default as upstream_
import clusterbank.cbank.controllers as controllers
from clusterbank.cbank.controllers import main, report_main, new_main, \
    report_users_main, report_projects_main, report_allocations_main, \
    report_holds_main, report_jobs_main, report_charges_main, \
    new_allocation_main, new_charge_main, new_hold_main, new_refund_main, \
    handle_exceptions, detail_jobs_main, import_main, import_jobs_main, \
    detail_charges_main, detail_refunds_main
from clusterbank.cbank.exceptions import UnknownCommand, \
    UnexpectedArguments, UnknownProject, MissingArgument, MissingResource, \
    NotPermitted, ValueError_, UnknownCharge

from nose.tools import assert_equal, assert_true


def current_username ():
    return pwd.getpwuid(os.getuid())[0]


def current_user ():
    return user_by_name(current_username())


class FakeDateTime (object):
    
    def __init__ (self, now):
        self._now = now
    
    def __call__ (self, *args):
        return datetime(*args)
    
    def now (self):
        return self._now


def assert_ident (obj1, obj2):
    assert obj1 is obj2, "%r is not %r" % (obj1, obj2)


class FakeFunc (object):
    
    def __init__ (self, func=lambda:None):
        self.calls = []
        self.func = func
    
    def __call__ (self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.func()


def run (func, args=None, stdin=None):
    if args is None:
        args = []
    if stdin is None:
        stdin = sys.stdin
    real_argv = sys.argv
    real_stdin = sys.stdin
    real_stdout = sys.stdout
    real_stderr = sys.stderr
    try:
        sys.argv = [func.__name__] + args
        sys.stdin = stdin
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
        sys.stdin = real_stdin
        sys.stdout = real_stdout
        sys.stderr = real_stderr


def be_admin ():
    current_user = current_username()
    clusterbank.config.set("cbank", "admins", current_user)


def not_admin ():
    clusterbank.config.remove_option("cbank", "admins")


def setup ():
    metadata.bind = create_engine("sqlite:///:memory:")
    current_user = current_username()
    upstream_.users = [
        upstream_.User(1, "user1"),
        upstream_.User(2, "user2"),
        upstream_.User(3, current_user)]
    upstream_.projects = [
        upstream_.Project(1, "project1"), upstream_.Project(2, "project2"),
        upstream_.Project(3, "project3"), upstream_.Project(4, "project4")]
    upstream_.projects[0].members.append(upstream_.users[0])
    upstream_.projects[1].members.append(upstream_.users[1])
    upstream_.projects[1].members.append(upstream_.users[2])
    upstream_.projects[2].members.append(upstream_.users[1])
    upstream_.projects[2].members.append(upstream_.users[2])
    upstream_.projects[2].admins.append(upstream_.users[2])
    upstream_.projects[3].members.append(upstream_.users[0])
    upstream_.projects[3].admins.append(upstream_.users[2])
    upstream_.resources = [
        upstream_.Resource(1, "resource1"), upstream_.Resource(2, "resource2")]
    upstream.use = upstream_
    fake_dt = FakeDateTime(datetime(2000, 1, 1))
    clusterbank.cbank.controllers.datetime = fake_dt


def teardown ():
    upstream_.users = []
    upstream_.projects = []
    upstream_.resources = []
    upstream.use = None
    Session.bind = None
    clusterbank.cbank.controllers.datetime = datetime


class TestHandleExceptionsDecorator (object):
    
    def test_cbank_exception (self):
        exceptions = [UnknownCommand, UnexpectedArguments, UnknownProject,
            MissingArgument, MissingResource, NotPermitted, ValueError_,
            UnknownCharge]
        for Ex in exceptions:
            @handle_exceptions
            def func ():
                raise Ex()
            code, stdout, stderr = run(func)
            assert_equal(code, Ex.exit_code)


class CbankTester (object):

    def setup (self):
        metadata.create_all()
        clusterbank.config.add_section("cbank")
        for user in upstream_.users:
            user_by_name(user.name)
        for project in upstream_.projects:
            project_by_name(project.name)
        for resource in upstream_.resources:
            resource_by_name(resource.name)
    
    def teardown (self):
        Session.remove()
        clusterbank.config.remove_section("cbank")
        metadata.drop_all()


class TestDetailCharges (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._print_charges = controllers.print_charges
        controllers.print_charges = FakeFunc()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.print_charges = self._print_charges
    
    def test_user (self):
        a = Allocation(project("project1"), resource("resource1"), 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        c = Charge(a, 0)
        c.jobs = [Job("resource1.1")]
        c.jobs[0].user = current_user()
        Session.add(c)
        Session.flush()
        run(detail_charges_main, ["%i" % c.id])
        args, kwargs = controllers.print_charges.calls[0]
        assert_equal(list(args[0]), [c])
    
    def test_non_user (self):
        a = Allocation(project("project1"), resource("resource1"), 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        c = Charge(a, 0)
        Session.add(c)
        Session.flush()
        run(detail_charges_main, ["%i" % c.id])
        args, kwargs = controllers.print_charges.calls[0]
        assert_equal(list(args[0]), [])
    
    def test_project_admin (self):
        a = Allocation(project("project3"), resource("resource1"), 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        c = Charge(a, 0)
        Session.add(c)
        Session.flush()
        run(detail_charges_main, ["%i" % c.id])
        args, kwargs = controllers.print_charges.calls[0]
        assert_equal(list(args[0]), [c])


class TestAdminDetailCharges (TestDetailCharges):
    
    def setup (self):
        TestDetailCharges.setup(self)
        be_admin()
    
    def test_non_user (self):
        a = Allocation(project("project1"), resource("resource1"), 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        c = Charge(a, 0)
        Session.add(c)
        Session.flush()
        run(detail_charges_main, ["%i" % c.id])
        args, kwargs = controllers.print_charges.calls[0]
        assert_equal(list(args[0]), [c])


class TestDetailRefunds (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._print_refunds = controllers.print_refunds
        controllers.print_refunds = FakeFunc()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.print_refunds = self._print_refunds
    
    def test_user (self):
        a = Allocation(project("project1"), resource("resource1"), 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        c = Charge(a, 0)
        c.jobs = [Job("resource1.1")]
        c.jobs[0].user = current_user()
        r = Refund(c)
        Session.add(r)
        Session.flush()
        run(detail_refunds_main, ["%i" % r.id])
        args, kwargs = controllers.print_refunds.calls[0]
        assert_equal(list(args[0]), [r])
    
    def test_non_user (self):
        a = Allocation(project("project1"), resource("resource1"), 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        c = Charge(a, 0)
        r = Refund(c)
        Session.add(r)
        Session.flush()
        run(detail_refunds_main, ["%i" % r.id])
        args, kwargs = controllers.print_refunds.calls[0]
        assert_equal(list(args[0]), [])
    
    def test_project_admin (self):
        a = Allocation(project("project3"), resource("resource1"), 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        c = Charge(a, 0)
        r = Refund(c)
        Session.add(r)
        Session.flush()
        run(detail_refunds_main, ["%i" % r.id])
        args, kwargs = controllers.print_refunds.calls[0]
        assert_equal(list(args[0]), [r])


class TestAdminDetailRefunds (TestDetailRefunds):
    
    def setup (self):
        TestDetailRefunds.setup(self)
        be_admin()
    
    def test_non_user (self):
        a = Allocation(project("project1"), resource("resource1"), 0,
            datetime(2000, 1, 1), datetime(2001, 1, 1))
        c = Charge(a, 0)
        r = Refund(c)
        Session.add(r)
        Session.flush()
        run(detail_refunds_main, ["%i" % r.id])
        args, kwargs = controllers.print_refunds.calls[0]
        assert_equal(list(args[0]), [r])


class TestMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._report_main = controllers.report_main
        self._new_main = controllers.new_main
        self._detail_main = controllers.detail_main
        controllers.report_main = FakeFunc()
        controllers.new_main = FakeFunc()
        controllers.detail_main = FakeFunc()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.report_main = self._report_main
        controllers.new_main = self._new_main
        controllers.detail_main = self._detail_main
    
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
    
    def test_detail (self):
        def test_ ():
            assert sys.argv[0] == "main detail", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.new_main.func = test_
        args = "detail 1 2 3"
        run(main, args.split())
        assert controllers.detail_main.calls
    
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
        args = "project1 1000 -r resource1 -s 2008-01-01 -e 2009-01-01 -c test"
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
            -e 2009-01-01 -c test asdf"""
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
        args = "project1 1000 -r resource1 -s 2008-01-01 -e 2009-01-01 -c test"
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
        args = "project1 1000 -r resource1 -s bad_start -e 2009-01-01 -c test"
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
        args = "project1 1000 -r resource1 -s 2008-01-01 -e bad_end -c test"
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
            -e 2009-01-01 -c test"""
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
        args = "1000 -r resource1 -s 2008-01-01 -e 2009-01-01 -c test"
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
        args = "project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -c test"
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
        args = "project1 1000 -r resource1 -e 2009-01-01 -c test"
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
        args = "project1 1000 -r resource1 -s 2000-01-01 -c test"
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
        args = "project1 1000 -s 2008-01-01 -e 2009-01-01 -c test"
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
        args = "project1 1000 -s 2008-01-01 -e 2009-01-01 -c test"
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
        args = "project1 1000 -r resource1 -s 2008-01-01 -e 2009-01-01 -c test"
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
        Session.add(allocation)
        Session.commit()
        args = "project1 100 -r resource1 -c test"
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
        Session.add(allocation)
        Session.commit()
        args = "project1 100 -r resource1 -c test asdf"
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
        Session.add(allocation)
        Session.commit()
        args = "project1 100 -r resource1 -c test"
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
        Session.add(allocation)
        Session.commit()
        args = "project1 100 -c test"
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
        Session.add(allocation)
        Session.commit()
        args = "project1 100 -c test"
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
        Session.add(allocation)
        Session.commit()
        args = "100 -r resource1 -c test"
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
        Session.add(allocation)
        Session.commit()
        args = "project1 -r resource1 -c test"
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
        Session.add(allocation)
        Session.commit()
        args = "project1 '-100' -r resource1 -c test"
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
        Session.add(allocation)
        Session.commit()
        args = "project1 100 -r resource1"
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
        Session.add(allocation)
        Session.commit()
        args = "project1 100 -r resource1 -c test"
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
        Session.add(allocation)
        Session.add(charge)
        Session.commit()
        args = "%s 50 -c test" % charge.id
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
        Session.add(allocation)
        Session.add(charge)
        Session.commit()
        args = "%s 50 -c test asdf" % charge.id
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
        Session.add(allocation)
        Session.add(charge)
        Session.commit()
        args = "%s 50 -c test" % charge.id
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
        Session.add(allocation)
        Session.add(charge)
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
        Session.add(allocation)
        Session.add(charge)
        Session.commit()
        args = "50 -c test"
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
        Session.add(allocation)
        Session.add(charge)
        Session.commit()
        args = "%s -c test" % charge.id
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
        Session.add(allocation)
        Session.add(charge)
        Session.add(refund)
        Session.commit()
        args = "%s -c test" % charge.id
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
        Session.add(allocation)
        Session.add(charge)
        Session.commit()
        args = "%s 50 -c test" % charge.id
        code, stdout, stderr = run(new_refund_main, args.split())
        Session.remove()
        assert not refunds.count(), "created a refund when not an admin"
        assert code == NotPermitted.exit_code, code


class TestImportJobs (CbankTester):

    def setup (self):
        CbankTester.setup(self)
        be_admin()
    
    def test_empty (self):
        stdin = StringIO()
        stdin.write("   \n  ")
        stdin.seek(0)
        code, stdout, stderr = run(import_jobs_main, [], stdin)
        assert_equal(code, 0)
        jobs = Session.query(Job)
        assert_equal(jobs.count(), 0)
    
    def test_comment (self):
        stdin = StringIO()
        stdin.write("# a comment\n# another comment")
        stdin.seek(0)
        code, stdout, stderr = run(import_jobs_main, [], stdin)
        assert_equal(code, 0)
        jobs = Session.query(Job)
        assert_equal(jobs.count(), 0)
    
    def test_job_from_pbs_q (self):
        entry = "04/18/2008 02:10:12;Q;692009.jmayor5.lcrc.anl.gov;queue=shared\n"
        stdin = StringIO()
        stdin.write(entry)
        stdin.seek(0)
        code, stdout, stderr = run(import_jobs_main, [], stdin)
        assert_equal(code, 0)
        jobs = Session.query(Job)
        assert_equal(jobs.count(), 1)
        job_ = jobs.one()
        assert_equal(job_.id, "692009.jmayor5.lcrc.anl.gov")
        assert_equal(job_.queue, "shared")
    
    def test_job_from_pbs_s (self):
        entry = "04/18/2008 02:10:12;S;692009.jmayor5.lcrc.anl.gov;user=user1 group=agroup account=project1 jobname=myjob queue=shared ctime=1208502612 qtime=1208502612 etime=1208502612 start=1208502612 exec_host=j340/0+j341/0+j342/0+j343/0+j344/0+j345/0+j346/0+j347/0 Resource_List.ncpus=8 Resource_List.neednodes=8 Resource_List.nodect=8 Resource_List.nodes=8 Resource_List.walltime=05:00:00"
        stdin = StringIO()
        stdin.write(entry)
        stdin.seek(0)
        code, stdout, stderr = run(import_jobs_main, [], stdin)
        assert_equal(code, 0)
        jobs = Session.query(Job)
        assert_equal(jobs.count(), 1)
        job_ = jobs.one()
        assert_equal(job_.id, "692009.jmayor5.lcrc.anl.gov")
        assert_ident(job_.user, user("user1"))
        assert_equal(job_.group, "agroup")
        assert_ident(job_.account, project("project1"))
        assert_equal(job_.name, "myjob")
        assert_equal(job_.queue, "shared")
        assert_equal(job_.ctime, datetime(2008, 4, 18, 2, 10, 12))
        assert_equal(job_.qtime, datetime(2008, 4, 18, 2, 10, 12))
        assert_equal(job_.etime, datetime(2008, 4, 18, 2, 10, 12))
        assert_equal(job_.start, datetime(2008, 4, 18, 2, 10, 12))
        assert_equal(job_.exec_host,
            "j340/0+j341/0+j342/0+j343/0+j344/0+j345/0+j346/0+j347/0")
        assert_equal(job_.resource_list, {'ncpus':8, 'neednodes':8,
            'nodect':8, 'nodes':8, 'walltime':timedelta(hours=5)})
    
    def test_job_from_pbs_e (self):
        entry = "04/18/2008 03:35:28;E;691908.jmayor5.lcrc.anl.gov;user=user1 group=agroup account=project1 jobname=myjob queue=pri4 ctime=1208378066 qtime=1208378066 etime=1208378066 start=1208378066 exec_host=j75/0+j76/0+j77/0+j78/0+j79/0+j86/0+j87/0+j88/0+j89/0+j90/0+j91/0+j93/0+j94/0+j100/0+j101/0+j102/0+j103/0+j104/0+j105/0+j106/0+j107/0+j108/0+j109/0+j110/0 Resource_List.ncpus=24 Resource_List.neednodes=24 Resource_List.nodect=24 Resource_List.nodes=24 Resource_List.walltime=36:00:00 session=23061 end=1208507728 Exit_status=265 resources_used.cpupercent=0 resources_used.cput=00:00:08 resources_used.mem=41684kb resources_used.ncpus=24 resources_used.vmem=95988kb resources_used.walltime=36:00:47"
        stdin = StringIO()
        stdin.write(entry)
        stdin.seek(0)
        code, stdout, stderr = run(import_jobs_main, [], stdin)
        assert_equal(code, 0)
        jobs = Session.query(Job)
        assert_equal(jobs.count(), 1)
        job_ = jobs.one()
        assert_equal(job_.id, "691908.jmayor5.lcrc.anl.gov")
        assert_ident(job_.user, user("user1"))
        assert_equal(job_.group, "agroup")
        assert_ident(job_.account, project("project1"))
        assert_equal(job_.name, "myjob")
        assert_equal(job_.queue, "pri4")
        assert_equal(job_.ctime, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.qtime, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.etime, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.start, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.exec_host, "j75/0+j76/0+j77/0+j78/0+j79/0+j86/0+j87/0+j88/0+j89/0+j90/0+j91/0+j93/0+j94/0+j100/0+j101/0+j102/0+j103/0+j104/0+j105/0+j106/0+j107/0+j108/0+j109/0+j110/0")
        assert_equal(job_.resource_list, {'ncpus':24, 'neednodes':24,
            'nodect':24, 'nodes':24, 'walltime':timedelta(hours=36)})
        assert_equal(job_.resources_used, {
            'walltime':timedelta(hours=36, seconds=47),
            'cput':timedelta(seconds=8), 'cpupercent':0, 'vmem':"95988kb",
            'ncpus':24, 'mem':"41684kb"})
        assert_equal(job_.session, 23061)
        assert_equal(job_.end, datetime(2008, 4, 18, 3, 35, 28))
        assert_equal(job_.exit_status, 265)
    
    def test_job_from_pbs_q_duplicate (self):
        entry1 = "04/18/2008 02:10:12;Q;692009.jmayor5.lcrc.anl.gov;queue=shared\n"
        entry2 = "04/18/2008 02:10:12;Q;692009.jmayor5.lcrc.anl.gov;queue=exclusive\n"
        stdin = StringIO()
        stdin.write(entry1 + entry2)
        stdin.seek(0)
        code, stdout, stderr = run(import_jobs_main, [], stdin)
        assert_equal(code, 0)
        jobs = Session.query(Job)
        assert_equal(jobs.count(), 1)
        job_ = jobs.one()
        assert_equal(job_.id, "692009.jmayor5.lcrc.anl.gov")
        assert_equal(job_.queue, "exclusive")


class TestReportMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._report_users_main = controllers.report_users_main
        self._report_projects_main = controllers.report_projects_main
        self._report_allocations_main = controllers.report_allocations_main
        self._report_holds_main = controllers.report_holds_main
        self._report_jobs_main = controllers.report_jobs_main
        self._report_charges_main = controllers.report_charges_main
        controllers.report_users_main = FakeFunc()
        controllers.report_projects_main = FakeFunc()
        controllers.report_allocations_main = FakeFunc()
        controllers.report_holds_main = FakeFunc()
        controllers.report_jobs_main = FakeFunc()
        controllers.report_charges_main = FakeFunc()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.report_users_main = self._report_users_main
        controllers.report_projects_main = self._report_projects_main
        controllers.report_allocations_main = self._report_allocations_main
        controllers.report_holds_main = self._report_holds_main
        controllers.report_jobs_main = self._report_jobs_main
        controllers.report_charges_main = self._report_charges_main
    
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
    
    def test_holds (self):
        def test_ ():
            assert sys.argv[0] == "report_main jobs", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        controllers.report_jobs_main.func = test_
        args = "jobs 1 2 3"
        run(report_main, args.split())
        assert controllers.report_jobs_main.calls
    
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
        user = current_user()
        projects = user.projects
        code, stdout, stderr = run(report_users_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        assert_equal(set(args[0]), set([user]))
        assert_equal(set(kwargs['projects']), set(projects))
    
    def test_self_users (self):
        user = current_user()
        projects = user.projects
        code, stdout, stderr = run(report_users_main,
            ("-u %s" % user.name).split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        assert_equal(set(args[0]), set([user]))
        assert_equal(set(kwargs['projects']), set(projects))
    
    def test_other_users (self):
        """cannot specify other users."""
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
    
    def test_project_admin_projects (self):
        project = project_by_name("project3")
        users = project.members
        code, stdout, stderr = run(report_users_main, "-p project3".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        assert_equal(set(args[0]), set(users))
        assert_equal(set(kwargs['projects']), set([project]))
    
    def test_project_admin_users (self):
        code, stdout, stderr = run(
            report_users_main, "-p project3 -u user1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        assert_equal(set(args[0]), set([user_by_name("user1")]))
        assert_equal(set(kwargs['projects']),
            set([project_by_name("project3")]))
    
    def test_resources (self):
        code, stdout, stderr = run(report_users_main, "-r resource1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        assert_equal(set(kwargs['resources']),
            set([resource_by_name("resource1")]))
        
    def test_after (self):
        code, stdout, stderr = run(report_users_main, "-a 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        assert_equal(kwargs['after'], datetime(2000, 1, 1))
    
    def test_before (self):
        code, stdout, stderr = run(report_users_main, "-b 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        assert_equal(kwargs['before'], datetime(2000, 1, 1))


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
    
    def test_self_users (self):
        user = current_user()
        projects = user.projects
        code, stdout, stderr = run(report_users_main,
            ("-u %s" % user.name).split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_users_report.calls[0]
        assert_equal(set(args[0]), set([user]))
        assert_equal(set(kwargs['projects']), set([]))


class TestProjectsReport (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._print_projects_report = controllers.print_projects_report
        controllers.print_projects_report = FakeFunc()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.print_projects_report = self._print_projects_report
    
    def test_default (self):
        """Current user's projects"""
        projects = current_user().projects
        code, stdout, stderr = run(report_projects_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(set(args[0]), set(projects))
    
    def test_member_projects (self):
        """a specific project of the user"""
        code, stdout, stderr = run(report_projects_main, "-p project2".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(set(args[0]), set([project_by_name("project2")]))
        
    def test_project_admin_projects (self):
        """a specific project the user admins"""
        code, stdout, stderr = run(report_projects_main, "-p project3".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(set(args[0]), set([project_by_name("project3")]))
    
    def test_other_projects (self):
        """cannot see other projects (not member, not admin)"""
        code, stdout, stderr = run(report_projects_main, "-p project1".split())
        assert_equal(code, NotPermitted.exit_code)
        assert not controllers.print_projects_report.calls
    
    def test_other_users (self):
        code, stdout, stderr = run(report_projects_main, "-u user1".split())
        assert_equal(code, NotPermitted.exit_code)
        assert not controllers.print_projects_report.calls

    def test_project_admin_users (self):
        code, stdout, stderr = run(
            report_projects_main, "-p project3 -u user1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(set(args[0]), set([project_by_name("project3")]))
        assert_equal(set(kwargs['users']), set([user_by_name("user1")]))
    
    def test_self_users (self):
        user = current_user()
        projects = user.projects
        code, stdout, stderr = run(
            report_projects_main, ("-u %s" % user.name).split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(set(kwargs['users']), set([user]))
    
    def test_resources (self):
        code, stdout, stderr = run(report_projects_main, "-r resource1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(set(kwargs['resources']),
            set([resource_by_name("resource1")]))
        
    def test_after (self):
        code, stdout, stderr = run(
            report_projects_main, "-a 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(kwargs['after'], datetime(2000, 1, 1))
    
    def test_before (self):
        code, stdout, stderr = run(
            report_projects_main, "-b 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(kwargs['before'], datetime(2000, 1, 1))

    def test_resources (self):
        code, stdout, stderr = run(report_projects_main, "-r resource1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(set(kwargs['resources']),
            set([resource_by_name("resource1")]))
        
    def test_after (self):
        code, stdout, stderr = run(
            report_projects_main, "-a 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(kwargs['after'], datetime(2000, 1, 1))
    
    def test_before (self):
        code, stdout, stderr = run(
            report_projects_main, "-b 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(kwargs['before'], datetime(2000, 1, 1))

class TestProjectsReport_Admin (TestProjectsReport):
    
    def setup (self):
        TestProjectsReport.setup(self)
        be_admin()
    
    def test_default (self):
        """all projects"""
        projects = Session.query(Project).all()
        code, stdout, stderr = run(report_projects_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(set(args[0]), set(projects))

    def test_other_projects (self):
        code, stdout, stderr = run(report_projects_main, "-p project3".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(set(args[0]), set([project_by_name("project3")]))
    
    def test_other_users (self):
        user = user_by_name("user1")
        code, stdout, stderr = run(report_projects_main, "-u user1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_projects_report.calls[0]
        assert_equal(set(kwargs['users']), set([user_by_name("user1")]))
        assert_equal(set(args[0]), set(user.projects))


def projects (users):
    return sum((user.projects for user in users), [])


def allocations (projects):
    return sum((project.allocations for project in projects), [])


def active (allocations):
    now = datetime(2000, 1, 1)
    return (allocation for allocation in allocations
        if allocation.start <= now and allocation.expiration > now)
 

class TestAllocationsReport (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._print_allocations_report = controllers.print_allocations_report
        controllers.print_allocations_report = FakeFunc()
        for project in Session().query(Project):
            Allocation(project, resource_by_name("resource1"), 0,
                datetime(1999, 1, 1), datetime(2000, 1, 1))
            Allocation(project, resource_by_name("resource1"), 0,
                datetime(1999, 1, 1), datetime(2001, 1, 1))
            Allocation(project, resource_by_name("resource2"), 0,
                datetime(2000, 1, 1), datetime(2001, 1, 1))
            Allocation(project, resource_by_name("resource2"), 0,
                datetime(2001, 1, 1), datetime(2002, 1, 1))
        Session.flush()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.print_allocations_report = self._print_allocations_report
    
    def test_default (self):
        """Current user's allocations"""
        projects = current_user().projects
        code, stdout, stderr = run(report_allocations_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]), set(active(allocations(projects))))
    
    def test_member_projects (self):
        """a specific project of the user"""
        code, stdout, stderr = run(
            report_allocations_main, "-p project2".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]),
            set(active(project_by_name("project2").allocations)))
        
    def test_project_admin_projects (self):
        """a specific project the user admins"""
        code, stdout, stderr = run(
            report_allocations_main, "-p project3".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]),
            set(active(project_by_name("project3").allocations)))
    
    def test_other_projects (self):
        """cannot see other projects (not member, not admin)"""
        code, stdout, stderr = run(
            report_allocations_main, "-p project1".split())
        assert_equal(code, NotPermitted.exit_code)
        assert not controllers.print_allocations_report.calls
    
    def test_other_users (self):
        code, stdout, stderr = run(report_allocations_main, "-u user1".split())
        assert_equal(code, NotPermitted.exit_code)
        assert not controllers.print_allocations_report.calls

    def test_project_admin_users (self):
        code, stdout, stderr = run(
            report_allocations_main, "-p project3 -u user1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]),
            set(active(project_by_name("project3").allocations)))
        assert_equal(set(kwargs['users']), set([user_by_name("user1")]))
    
    def test_self_users (self):
        user = current_user()
        projects = user.projects
        code, stdout, stderr = run(
            report_allocations_main, ("-u %s" % user.name).split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(kwargs['users']), set([user]))
    
    def test_resources (self):
        allocations_ = active([allocation
            for allocation in allocations(current_user().projects)
            if allocation.resource == resource_by_name("resource1")])
        code, stdout, stderr = run(
            report_allocations_main, "-r resource1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]), set(allocations_))
        
    def test_after (self):
        allocations_ = allocations(current_user().projects)
        allocations_ = [a for a in allocations_
            if a.expiration > datetime(2001, 1, 1)]
        code, stdout, stderr = run(
            report_allocations_main, "-a 2001-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]), set(allocations_))
        assert_equal(kwargs['after'], datetime(2001, 1, 1))
    
    def test_before (self):
        allocations_ = allocations(current_user().projects)
        allocations_ = [a for a in allocations_
            if a.start <= datetime(2000, 1, 1)]
        code, stdout, stderr = run(
            report_allocations_main, "-b 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]), set(allocations_))
        assert_equal(kwargs['before'], datetime(2000, 1, 1))
    
    def test_comments (self):
        code, stdout, stderr = run(
            report_allocations_main, ["-c"])
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_true(kwargs['comments'])


class TestAllocationsReport_Admin (TestAllocationsReport):
    
    def setup (self):
        TestAllocationsReport.setup(self)
        be_admin()
    
    def test_default (self):
        """all active allocations"""
        allocations = active(Session.query(Allocation))
        code, stdout, stderr = run(report_allocations_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]), set(allocations))

    def test_other_projects (self):
        code, stdout, stderr = run(
            report_allocations_main, "-p project3".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]),
            set(active(project_by_name("project3").allocations)))
    
    def test_other_users (self):
        user = user_by_name("user1")
        code, stdout, stderr = run(report_allocations_main, "-u user1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(kwargs['users']), set([user_by_name("user1")]))
        assert_equal(set(args[0]),
            set(active(allocations(user.projects))))
    
    def test_resources (self):
        allocations_ = active([allocation
            for allocation in Session.query(Allocation).all()
            if allocation.resource == resource_by_name("resource1")])
        code, stdout, stderr = run(
            report_allocations_main, "-r resource1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]), set(allocations_))

    def test_after (self):
        allocations_ = [a for a in Session.query(Allocation)
            if a.expiration > datetime(2001, 1, 1)]
        code, stdout, stderr = run(
            report_allocations_main, "-a 2001-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]), set(allocations_))
        assert_equal(kwargs['after'], datetime(2001, 1, 1))
    
    def test_before (self):
        allocations_ = [a for a in Session.query(Allocation)
            if a.start <= datetime(2000, 1, 1)]
        code, stdout, stderr = run(
            report_allocations_main, "-b 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_allocations_report.calls[0]
        assert_equal(set(args[0]), set(allocations_))
        assert_equal(kwargs['before'], datetime(2000, 1, 1))


class TestHoldsReport (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._print_holds_report = controllers.print_holds_report
        controllers.print_holds_report = FakeFunc()
        user1, user2 = [user_by_name(user) for user in ["user1", "user2"]]
        for project in Session().query(Project):
            Allocation(project, resource_by_name("resource1"), 0,
                datetime(2000, 1, 1), datetime(2001, 1, 1))
            Allocation(project, resource_by_name("resource2"), 0,
                datetime(2000, 1, 1), datetime(2001, 1, 1))
        for allocation in Session().query(Allocation):
            h1 = Hold(allocation, 0)
            h1.datetime = datetime(2000, 1, 1)
            h1.user = user1
            h2 = Hold(allocation, 0)
            h2.datetime = datetime(1999, 1, 1)
            h2.user = user2
            h3 = Hold(allocation, 0)
            h3.datetime = datetime(1999, 1, 1)
            h3.active = False
            h2.user = current_user()
            h4 = Hold(allocation, 0)
            h4.datetime = datetime(2001, 1, 1)
            h2.user = current_user()
        Session.flush()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.print_holds_report = self._print_holds_report
    
    def test_default (self):
        holds = Session().query(Hold).filter_by(
            user=current_user(), active=True).filter(Hold.allocation.has(
            Allocation.project.has(Project.id.in_(project.id for project in
            current_user().projects))))
        code, stdout, stderr = run(report_holds_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_other_users (self):
        code, stdout, stderr = run(report_holds_main,
            "-p project1 -u user1".split())
        assert_equal(code, NotPermitted.exit_code)
        assert not controllers.print_holds_report.calls

    def test_member_users (self):
        code, stdout, stderr = run(report_holds_main,
            "-p project2 -u user1".split())
        assert_equal(code, NotPermitted.exit_code)
        assert not controllers.print_holds_report.calls
    
    def test_project_admin_users (self):
        holds = Session().query(Hold).filter_by(active=True,
            user=user_by_name("user1")).filter(Hold.allocation.has(
            Allocation.project == project_by_name("project4")))
        code, stdout, stderr = run(report_holds_main,
            "-u user1 -p project4".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_self_users (self):
        user = current_user()
        holds = Session().query(Hold).filter_by(
            user=user, active=True).filter(Hold.allocation.has(
            Allocation.project.has(Project.id.in_(project.id
                for project in user.projects))))
        code, stdout, stderr = run(report_holds_main, ("-u %s" % user).split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_member_projects (self):
        holds = Session().query(Hold).filter_by(
            user=current_user(), active=True).filter(Hold.allocation.has(
            Allocation.project==project_by_name("project2")))
        code, stdout, stderr = run(report_holds_main, "-p project2".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_project_admin_projects (self):
        holds = Session().query(Hold).filter_by(active=True).filter(
            Hold.allocation.has(
            Allocation.project == project_by_name("project4")))
        code, stdout, stderr = run(report_holds_main, "-p project4".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_other_projects (self):
        holds = Session().query(Hold).filter_by(
            user=current_user(), active=True).filter(Hold.allocation.has(
            Allocation.project==project_by_name("project1")))
        code, stdout, stderr = run(report_holds_main, "-p project1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_resources (self):
        holds = Session().query(Hold).filter_by(
            user=current_user(), active=True).filter(Hold.allocation.has(
            Allocation.project.has(Project.id.in_(project.id for project in
            current_user().projects)))).filter(Hold.allocation.has(
            Allocation.resource == resource_by_name("resource1")))
        code, stdout, stderr = run(report_holds_main, "-r resource1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_after (self):
        holds = Session().query(Hold).filter_by(
            user=current_user(), active=True).filter(Hold.allocation.has(
            Allocation.project.has(Project.id.in_(project.id for project in
            current_user().projects)))).filter(
            Hold.datetime >= datetime(2000, 1, 1))
        code, stdout, stderr = run(report_holds_main, "-a 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_before (self):
        holds = Session().query(Hold).filter_by(
            user=current_user(), active=True).filter(Hold.allocation.has(
            Allocation.project.has(Project.id.in_(project.id for project in
            current_user().projects)))).filter(
            Hold.datetime < datetime(2000, 1, 1))
        code, stdout, stderr = run(report_holds_main, "-b 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_comments (self):
        code, stdout, stderr = run(
            report_holds_main, ["-c"])
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_true(kwargs['comments'])


class TestHoldsReport_Admin (TestHoldsReport):
    
    def setup (self):
        TestHoldsReport.setup(self)
        be_admin()
    
    def test_self_users (self):
        user = current_user()
        holds = Session().query(Hold).filter_by(user=user, active=True)
        code, stdout, stderr = run(report_holds_main, ("-u %s" % user).split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_resources (self):
        holds = Session().query(Hold).filter_by(
            active=True).filter(Hold.allocation.has(
            Allocation.resource == resource_by_name("resource1")))
        code, stdout, stderr = run(report_holds_main, "-r resource1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_other_users (self):
        holds = Session().query(Hold).filter_by(active=True,
            user=user_by_name("user1")).filter(Hold.allocation.has(
            Allocation.project == project_by_name("project1")))
        code, stdout, stderr = run(report_holds_main,
            "-p project1 -u user1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_other_projects (self):
        holds = Session().query(Hold).filter_by(
            active=True).filter(Hold.allocation.has(
            Allocation.project==project_by_name("project1")))
        code, stdout, stderr = run(report_holds_main, "-p project1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_member_users (self):
        holds = Session().query(Hold).filter_by(user=user_by_name("user1"),
            active=True).filter(Hold.allocation.has(
            Allocation.project==project_by_name("project2")))
        code, stdout, stderr = run(report_holds_main,
            "-p project2 -u user1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
     
    def test_member_projects (self):
        holds = Session().query(Hold).filter_by(
            active=True).filter(Hold.allocation.has(
            Allocation.project==project_by_name("project2")))
        code, stdout, stderr = run(report_holds_main, "-p project2".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_default (self):
        holds = Session().query(Hold).filter_by(active=True)
        code, stdout, stderr = run(report_holds_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_after (self):
        holds = Session().query(Hold).filter_by(
            active=True).filter(Hold.datetime >= datetime(2000, 1, 1))
        code, stdout, stderr = run(report_holds_main, "-a 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))
    
    def test_before (self):
        holds = Session().query(Hold).filter_by(
            active=True).filter(Hold.datetime < datetime(2000, 1, 1))
        code, stdout, stderr = run(report_holds_main, "-b 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_holds_report.calls[0]
        assert_equal(set(args[0]), set(holds))


class TestJobsReport (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._print_jobs_report = controllers.print_jobs_report
        controllers.print_jobs_report = FakeFunc()
        current_user_ = current_user()
        clusterbank.config.add_section("resources")
        clusterbank.config.set("resources", "resource1", r"resource1\..*")
        clusterbank.config.set("resources", "resource2", r"resource2\..*")
        j1 = Job("resource1.1")
        j1.user = current_user_
        j1.start = datetime(2000, 1, 1)
        j1.end = datetime(2000, 1, 2)
        j1.account = project_by_name("project2")
        j1.ctime = datetime(2000, 1, 1)
        j2 = Job("resource1.2")
        j2.user = current_user_
        j2.start = datetime(2000, 1, 30)
        j2.end = datetime(2000, 2, 2)
        j2.ctime = datetime(2000, 1, 2)
        j2.account = project_by_name("project2")
        j3 = Job("resource1.3")
        j3.user = current_user_
        j3.start = datetime(2000, 2, 1)
        j3.account = project_by_name("project2")
        j3.ctime = datetime(2000, 1, 4)
        j4 = Job("resource1.4")
        j4.user = current_user_
        j4.ctime = datetime(2000, 1, 5)
        j5 = Job("resource1.5")
        j5.user = user_by_name("user1")
        j5.start = datetime(2000, 1, 30)
        j5.end = datetime(2000, 2, 2)
        j5.account = project_by_name("project4")
        j5.ctime = datetime(2000, 1, 6)
        j6 = Job("resource1.6")
        j6.start = datetime(2000, 2, 1)
        j6.ctime = datetime(2000, 1, 7)
        j7 = Job("resource1.7")
        j7.account = project_by_name("project2")
        j7.user = current_user_
        j7.ctime = datetime(2000, 1, 8)
        j8 = Job("resource1.8")
        j8.account = project_by_name("project2")
        j8.user = user_by_name("user2")
        j8.ctime = datetime(2000, 1, 9)
        j9 = Job("resource1.9")
        j9.account = project_by_name("project2")
        j9.ctime = datetime(2000, 1, 10)
        j10 = Job("resource1.10")
        j10.account = project_by_name("project4")
        j10.user = current_user_
        j10.ctime = datetime(2000, 1, 11)
        j11 = Job("resource1.11")
        j11.account = project_by_name("project4")
        j11.user = user_by_name("user1")
        j11.ctime = datetime(2000, 1, 12)
        j12 = Job("resource1.12")
        j12.account = project_by_name("project4")
        j12.ctime = datetime(2000, 1, 13)
        j13 = Job("resource1.13")
        j13.account = project_by_name("project1")
        j13.user = current_user_
        j13.ctime = datetime(2000, 1, 14)
        j14 = Job("resource1.14")
        j14.user = user_by_name("user1")
        j14.account = project_by_name("project1")
        j14.ctime = datetime(2000, 1, 15)
        j15 = Job("resource1.15")
        j15.user = user_by_name("user1")
        j15.account = project_by_name("project2")
        j15.ctime = datetime(2000, 1, 16)
        j1_2 = Job("resource2.1")
        j1_2.user = current_user_
        j1_2.account = project_by_name("project2")
        j1_2.ctime = datetime(2000, 1, 3)
        jobs = [j1, j2, j3, j4, j5, j6, j7, j8, j9, j10, j11, j12, j13, j14,
            j15, j1_2]
        for job_ in jobs:
            Session.add(job_)
        Session.flush()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.print_jobs_report = self._print_jobs_report
        clusterbank.config.remove_section("resources")
    
    def test_default (self):
        code, stdout, stderr = run(report_jobs_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_jobs_report.calls[0]
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.1",
            "resource1.2", "resource1.3", "resource1.7", "resource2.1"]))
        assert_equal(set(args[0]), set(jobs))
    
    def test_default_order (self):
        code, stdout, stderr = run(report_jobs_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_jobs_report.calls[0]
        s = Session()
        jobs = [s.query(Job).filter_by(id="resource1.1").one(),
            s.query(Job).filter_by(id="resource1.2").one(),
            s.query(Job).filter_by(id="resource2.1").one(),
            s.query(Job).filter_by(id="resource1.3").one(),
            s.query(Job).filter_by(id="resource1.7").one()]
        assert_equal(list(args[0]), jobs)
    
    def test_after (self):
        code, stdout, stderr = run(report_jobs_main, "-a 2000-02-01".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_([
            "resource1.2", "resource1.3"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
    
    def test_before (self):
        code, stdout, stderr = run(report_jobs_main, "-b 2000-02-01".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.1",
            "resource1.2"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
    
    def test_other_users (self):
        code, stdout, stderr = run(report_jobs_main,
            "-p project1 -u user1".split())
        assert_equal(code, NotPermitted.exit_code)
        assert not controllers.print_jobs_report.calls

    def test_member_users (self):
        code, stdout, stderr = run(report_jobs_main,
            "-p project2 -u user1".split())
        assert_equal(code, NotPermitted.exit_code)
        assert not controllers.print_jobs_report.calls
    
    def test_project_admin_users (self):
        code, stdout, stderr = run(report_jobs_main,
            "-u user1 -p project4".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_([
            "resource1.5", "resource1.11"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
     
    def test_self_users (self):
        user = current_user()
        code, stdout, stderr = run(report_jobs_main, ("-u %s" % user).split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.1",
            "resource1.2", "resource1.3", "resource1.7", "resource2.1"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
    
    def test_member_projects (self):
        code, stdout, stderr = run(report_jobs_main, "-p project2".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.1",
            "resource1.2", "resource1.3", "resource1.7", "resource2.1"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
    
    def test_project_admin_projects (self):
        code, stdout, stderr = run(report_jobs_main, "-p project4".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.5",
            "resource1.10", "resource1.11", "resource1.12"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
    
    def test_other_projects (self):
        code, stdout, stderr = run(report_jobs_main, "-p project1".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.13"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
    
    def test_resources (self):
        code, stdout, stderr = run(report_jobs_main, "-r resource2".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource2.1"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))


class TestJobsReport_Admin (TestJobsReport):
    
    def setup (self):
        TestJobsReport.setup(self)
        be_admin()

    def test_default (self):
        code, stdout, stderr = run(report_jobs_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_jobs_report.calls[0]
        jobs = Session().query(Job)
        assert_equal(set(args[0]), set(jobs))
    
    def test_default_order (self):
        code, stdout, stderr = run(report_jobs_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_jobs_report.calls[0]
        s = Session()
        jobs = [s.query(Job).filter_by(id="resource1.1").one(),
            s.query(Job).filter_by(id="resource1.2").one(),
            s.query(Job).filter_by(id="resource2.1").one(),
            s.query(Job).filter_by(id="resource1.3").one(),
            s.query(Job).filter_by(id="resource1.4").one(),
            s.query(Job).filter_by(id="resource1.5").one(),
            s.query(Job).filter_by(id="resource1.6").one(),
            s.query(Job).filter_by(id="resource1.7").one(),
            s.query(Job).filter_by(id="resource1.8").one(),
            s.query(Job).filter_by(id="resource1.9").one(),
            s.query(Job).filter_by(id="resource1.10").one(),
            s.query(Job).filter_by(id="resource1.11").one(),
            s.query(Job).filter_by(id="resource1.12").one(),
            s.query(Job).filter_by(id="resource1.13").one(),
            s.query(Job).filter_by(id="resource1.14").one(),
            s.query(Job).filter_by(id="resource1.15").one()]
        assert_equal(list(args[0]), jobs)
    
    def test_self_users (self):
        user = current_user()
        code, stdout, stderr = run(report_jobs_main, ("-u %s" % user).split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.1",
            "resource1.2", "resource1.3", "resource1.4", "resource1.7",
            "resource1.10", "resource1.13", "resource2.1"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
    
    def test_member_projects (self):
        code, stdout, stderr = run(report_jobs_main, "-p project2".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.1",
            "resource1.2", "resource1.3", "resource1.7", "resource1.8",
            "resource1.9", "resource1.15", "resource2.1"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
    
    def test_after (self):
        code, stdout, stderr = run(report_jobs_main, "-a 2000-02-01".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.2",
            "resource1.3", "resource1.5", "resource1.6"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
    
    def test_before (self):
        code, stdout, stderr = run(report_jobs_main, "-b 2000-02-01".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.1",
            "resource1.2", "resource1.5"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))

    def test_member_users (self):
        code, stdout, stderr = run(report_jobs_main,
            "-p project2 -u user1".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.15"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
    
    def test_other_users (self):
        code, stdout, stderr = run(report_jobs_main,
            "-p project1 -u user1".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.14"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))
    
    def test_other_projects (self):
        code, stdout, stderr = run(report_jobs_main, "-p project1".split())
        assert_equal(code, 0)
        jobs = Session().query(Job).filter(Job.id.in_(["resource1.13",
            "resource1.14"]))
        args, kwargs = controllers.print_jobs_report.calls[0]
        assert_equal(set(args[0]), set(jobs))


class TestChargesReport (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._print_charges_report = controllers.print_charges_report
        controllers.print_charges_report = FakeFunc()
        user1, user2 = [user_by_name(user) for user in ["user1", "user2"]]
        for project in Session().query(Project):
            Allocation(project, resource_by_name("resource1"), 0,
                datetime(2000, 1, 1), datetime(2001, 1, 1))
            Allocation(project, resource_by_name("resource2"), 0,
                datetime(2000, 1, 1), datetime(2001, 1, 1))
        for allocation in Session().query(Allocation):
            c1 = Charge(allocation, 0)
            c1.datetime = datetime(2000, 1, 1)
            c1.jobs = [Job("1.%i.%s" % (allocation.id, allocation.resource))]
            c1.jobs[0].user = user1
            c2 = Charge(allocation, 0)
            c2.datetime = datetime(1999, 1, 1)
            c2.jobs = [Job("2.%i.%s" % (allocation.id, allocation.resource))]
            c2.jobs[0].user = user2
            c3 = Charge(allocation, 0)
            c3.datetime = datetime(1999, 1, 1)
            c3.jobs = [Job("3.%i.%s" % (allocation.id, allocation.resource))]
            c3.jobs[0].user = current_user()
            c4 = Charge(allocation, 0)
            c4.datetime = datetime(2001, 1, 1)
            c4.jobs = [Job("4.%i.%s" % (allocation.id, allocation.resource))]
            c4.jobs[0].user = current_user()
        Session.flush()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.print_charges_report = self._print_charges_report
    
    def test_default (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([11, 12, 15, 16, 19, 20, 23, 24]))
        code, stdout, stderr = run(report_charges_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_other_users (self):
        code, stdout, stderr = run(report_charges_main,
            "-p project1 -u user1".split())
        assert_equal(code, NotPermitted.exit_code)
        assert not controllers.print_charges_report.calls

    def test_member_users (self):
        code, stdout, stderr = run(report_charges_main,
            "-p project2 -u user1".split())
        assert_equal(code, NotPermitted.exit_code)
        assert not controllers.print_charges_report.calls
    
    def test_project_admin_users (self):
        charges = Session().query(Charge).filter(Charge.id.in_([25, 29]))
        code, stdout, stderr = run(report_charges_main,
            "-u user1 -p project4".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_self_users (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([11, 12, 15, 16, 19, 20, 23, 24]))
        code, stdout, stderr = run(report_charges_main,
            ("-u %s" % current_user()).split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_member_projects (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([11, 12, 15, 16]))
        code, stdout, stderr = run(report_charges_main, "-p project2".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_project_admin_projects (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([25, 26, 27, 28, 29, 30, 31, 32]))
        code, stdout, stderr = run(report_charges_main, "-p project4".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_other_projects (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([3, 4, 7, 8]))
        code, stdout, stderr = run(report_charges_main, "-p project1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_resources (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([11, 12, 19, 20]))
        code, stdout, stderr = run(report_charges_main, "-r resource1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_after (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([12, 16, 20, 24]))
        code, stdout, stderr = run(report_charges_main,
            "-a 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_before (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([11, 15, 19, 23]))
        code, stdout, stderr = run(
            report_charges_main, "-b 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_comments (self):
        code, stdout, stderr = run(
            report_charges_main, ["-c"])
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_true(kwargs['comments'])


class TestChargesReport_Admin (TestChargesReport):
    
    def setup (self):
        TestChargesReport.setup(self)
        Charge.__repr__ = lambda self: str(self.id)
        be_admin()
    
    def test_self_users (self):
        charges = Session().query(Charge).filter(Charge.id.in_([
            20, 4, 11, 27, 24, 15, 8, 31, 3, 12, 19, 16, 23, 32, 28, 7]))
        code, stdout, stderr = run(report_charges_main,
            ("-u %s" % current_user()).split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_resources (self):
        charges = Session().query(Charge).filter(Charge.id.in_([
            3, 2, 26, 28, 19, 9, 20, 10, 25, 1, 27, 11, 18, 12, 17, 4]))
        code, stdout, stderr = run(
            report_charges_main, "-r resource1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_other_users (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([1, 5]))
        code, stdout, stderr = run(report_charges_main,
            "-p project1 -u user1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_other_projects (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([8, 2, 7, 1, 3, 5, 4, 6]))
        code, stdout, stderr = run(report_charges_main, "-p project1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_member_users (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([9, 13]))
        code, stdout, stderr = run(report_charges_main,
            "-p project2 -u user1".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
     
    def test_member_projects (self):
        charges = Session().query(Charge).filter(
            Charge.id.in_([10, 16, 14, 13, 9, 15, 11, 12]))
        code, stdout, stderr = run(report_charges_main, "-p project2".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_default (self):
        charges = Session().query(Charge)
        code, stdout, stderr = run(report_charges_main)
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_after (self):
        charges = Session().query(Charge).filter(Charge.id.in_([
            28, 4, 25, 24, 21, 16, 9, 29, 8, 13, 1, 20, 5, 12, 32, 17]))
        code, stdout, stderr = run(
            report_charges_main, "-a 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))
    
    def test_before (self):
        charges = Session().query(Charge).filter(Charge.id.in_([
            3, 11, 18, 10, 31, 15, 23, 2, 27, 6, 7, 26, 19, 14, 22, 30]))
        code, stdout, stderr = run(
            report_charges_main, "-b 2000-01-01".split())
        assert_equal(code, 0)
        args, kwargs = controllers.print_charges_report.calls[0]
        assert_equal(set(args[0]), set(charges))


class TestDetailJobs (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        be_admin()
        job_ = Job("resource1.1")
        Session.add(job_)
        self._print_jobs = controllers.print_jobs
        controllers.print_jobs = FakeFunc()
    
    def teardown (self):
        CbankTester.teardown(self)
        controllers.print_jobs = self._print_jobs
    
    def test_jobs_report (self):
        code, stdout, stderr = run(
            detail_jobs_main, "resource1.1".split())
        assert_equal(code, 0)
        jobs = Session.query(Job).filter(Job.id.in_(["resource1.1"]))
        args, kwargs = controllers.print_jobs.calls[0]
        assert_equal(set(args[0]), set(jobs))
 
