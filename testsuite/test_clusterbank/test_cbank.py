from nose.tools import raises

import sys
import pwd
import os
from datetime import datetime, timedelta
from StringIO import StringIO

from sqlalchemy import create_engine

from clusterbank.model import Session, Allocation, Charge, Refund, \
    user_by_name, project_by_name, resource_by_name
import clusterbank.upstreams.default as upstream
import clusterbank.cbank.controllers
import clusterbank.cbank.exceptions

def get_current_username ():
    return pwd.getpwuid(os.getuid())[0]

def setup ():
    clusterbank.model.metadata.bind = create_engine("sqlite:///:memory:")
    upstream.metadata.bind = create_engine("sqlite:///:memory:", echo=True)
    upstream.metadata.create_all()
    populate_upstream()
    clusterbank.model.upstream.use = upstream

def populate_upstream ():
    upstream.Session.save(upstream.Project(id=1, name="project1"))
    upstream.Session.save(upstream.Resource(id=1, name="resource1"))
    upstream.Session.save(upstream.User(id=1, name="user1"))
    current_user = get_current_username()
    upstream.Session.save(upstream.User(id=2, name=current_user))
    upstream.Session.commit()
    upstream.Session.remove()

def teardown ():
    upstream.metadata.drop_all()
    upstream.metadata.bind = None
    clusterbank.model.upstream.use = None
    clusterbank.model.metadata.bind = None

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


class CbankTester (object):

    def setup (self):
        clusterbank.model.metadata.create_all()
        current_user = get_current_username()
        clusterbank.config.add_section("cbank")
        clusterbank.config.set("cbank", "admins", current_user)
        self.fake_called = False
    
    def teardown (self):
        clusterbank.model.metadata.drop_all()
        Session.remove()
        upstream.Session.remove()
        clusterbank.config.remove_section("cbank")


class TestMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._report_main = clusterbank.cbank.controllers.report_main
        self._new_main = clusterbank.cbank.controllers.new_main
    
    def teardown (self):
        CbankTester.teardown(self)
        clusterbank.cbank.controllers.report_main = self._report_main
        clusterbank.cbank.controllers.new_main = self._new_main
    
    def test_exists_and_callable (self):
        assert hasattr(clusterbank.cbank.controllers, "main"), "main does not exist"
        assert callable(clusterbank.cbank.controllers.main), "main is not callable"
    
    def test_report (self):
        args = "report 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "main report"
            assert sys.argv[1:] == args.split()[1:], sys.argv
        clusterbank.cbank.controllers.report_main = fake
        run(clusterbank.cbank.controllers.main, args.split())
        assert self.fake_called
    
    def test_new (self):
        args = "new 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "main new", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        clusterbank.cbank.controllers.new_main = fake
        run(clusterbank.cbank.controllers.main, args.split())
        assert self.fake_called
    
    def test_default (self):
        args = "1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "main"
            assert sys.argv[1:] == args.split(), sys.argv
        clusterbank.cbank.controllers.report_main = fake
        run(clusterbank.cbank.controllers.main, args.split())
        assert self.fake_called
    
    def test_invalid (self):
        args = "invalid_command 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "main"
            assert sys.argv[1:] == args.split(), sys.argv
        clusterbank.cbank.controllers.report_main = fake
        run(clusterbank.cbank.controllers.main, args.split())
        assert self.fake_called


class TestNewMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._new_allocation_main = \
            clusterbank.cbank.controllers.new_allocation_main
        self._new_charge_main = \
            clusterbank.cbank.controllers.new_charge_main
        self._new_refund_main = \
            clusterbank.cbank.controllers.new_refund_main
    
    def teardown (self):
        CbankTester.teardown(self)
        clusterbank.cbank.controllers.new_allocation_main = \
            self._new_allocation_main
        clusterbank.cbank.controllers.new_charge_main = \
            self._new_charge_main
        clusterbank.cbank.controllers.new_refund_main = \
            self._new_refund_main
    
    def test_exists_and_callable (self):
        assert hasattr(clusterbank.cbank.controllers, "new_main"), "new_main does not exist"
        assert callable(clusterbank.cbank.controllers.new_main), "new_main is not callable"
    
    def test_allocation (self):
        args = "allocation 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "new_main allocation", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        clusterbank.cbank.controllers.new_allocation_main = fake
        run(clusterbank.cbank.controllers.new_main, args.split())
        assert self.fake_called
    
    def test_charge (self):
        args = "charge 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "new_main charge", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        clusterbank.cbank.controllers.new_charge_main = fake
        run(clusterbank.cbank.controllers.new_main, args.split())
        assert self.fake_called
    
    def test_refund (self):
        args = "refund 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "new_main refund", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        clusterbank.cbank.controllers.new_refund_main = fake
        run(clusterbank.cbank.controllers.new_main, args.split())
        assert self.fake_called
        
    
    def test_invalid (self):
        args = "invalid 1 2 3"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_main, args.split())
        assert code != 0, code


class TestNewAllocationMain (CbankTester):
    
    def test_exists_and_callable (self):
        assert hasattr(clusterbank.cbank.controllers, "new_allocation_main"), "new_allocation_main does not exist"
        assert callable(clusterbank.cbank.controllers.new_allocation_main), "new_allocation_main is not callable"
    
    def test_complete (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a 1000 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert query.count() == 1, "didn't create an allocation"
        allocation = query.one()
        assert allocation.start == datetime(2008, 1, 1), allocation.start
        assert allocation.expiration == datetime(2009, 1, 1), allocation.expiration
        assert allocation.amount == 1000, allocation.amount
        assert allocation.comment == "test", allocation.comment
        assert code == 0, code
    
    def test_unknown_arguments (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a 1000 -m test asdf"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count()
        assert code != 0, code
    
    def test_with_defined_units (self):
        clusterbank.config.set("cbank", "unit_factor", "1/2")
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a 1000 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert query.count() == 1, "didn't create an allocation"
        allocation = query.one()
        assert allocation.start == datetime(2008, 1, 1), allocation.start
        assert allocation.expiration == datetime(2009, 1, 1), allocation.expiration
        assert allocation.amount == 2000, allocation.amount
        assert allocation.comment == "test", allocation.comment
        assert code == 0, code
    
    def test_with_bad_start (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -s bad_start -e 2009-01-01 -a 1000 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created an allocation with bad start"
        assert code != 0, code
    
    def test_with_bad_end (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e bad_end -a 1000 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created an allocation with bad end"
        assert code != 0, code
    
    def test_with_bad_amount (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a bad_amount -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created an allocation with bad amount"
        assert code != 0, code

    def test_with_negative_amount (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        allocations = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not allocations.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a -1000 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not allocations.count(), "created an allocation with negative amount"
        assert code != 0, code
    
    def test_without_comment (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a 1000"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert query.count() == 1, "didn't create an allocation"
        allocation = query.one()
        assert allocation.start == datetime(2008, 1, 1), allocation.start
        assert allocation.expiration == datetime(2009, 1, 1), allocation.expiration
        assert allocation.amount == 1000, allocation.amount
        assert allocation.comment is None, allocation.comment
        assert code == 0, code
    
    def test_without_project (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-r resource1 -s 2008-01-01 -e 2009-01-01 -a 1000 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created allocation without project: %s" % new_allocations
        assert code != 0, code
    
    def test_without_amount (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created allocation without amount"
        assert code != 0, code
    
    def test_without_start (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -e 2009-01-01 -a 1000 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created allocation without start"
        sys.stdout.write(stderr.read())
        assert code != 0, code
    
    def test_without_expiration (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -a 1000 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created allocation without expiration"
        sys.stdout.write(stderr.read())
        assert code != 0, code

    def test_without_project_or_resource (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-s 2008-01-01 -e 2009-01-01 -a 1000 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created allocation without project or resource: %s" % new_allocations
        assert code != 0, code
    
    def test_without_resource (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -s 2008-01-01 -e 2009-01-01 -a 1000 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created allocation without resource: %s" % new_allocations
        assert code != 0, code

    def test_non_admin (self):
        clusterbank.config.set("cbank", "admins", "")
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert not query.count(), "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a 1000 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_allocation_main, args.split())
        Session.remove()
        assert not query.count(), "created allocation when not admin: %s" % new_allocations
        assert code != 0


class TestNewChargeMain (CbankTester):
    
    def test_exists_and_callable (self):
        assert hasattr(clusterbank.cbank.controllers, "new_charge_main"), "new_charge_main does not exist"
        assert callable(clusterbank.cbank.controllers.new_charge_main), "new_charge_main is not callable"
    
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
        args = "-p project1 -r resource1 -a 100 -m test -u user1"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_charge_main, args.split())
        assert code == 0
        assert charges.count() == 1, "didn't create a charge"
        charge = charges.one()
        assert charge.allocation is allocation, "incorrect allocation: %r" % charge.allocation
        assert charge.amount == 100, "incorrect charge amount: %i" % charge.amount
        assert charge.comment == "test", "incorrect comment: %s" % charge.comment
        assert charge.user is user, "incorrect user on charge: %s" % charge.user
    
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
        args = "-p project1 -r resource1 -a 100 -m test -u user1 asdf"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_charge_main, args.split())
        assert not charges.count()
        assert code != 0, code
    
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
        args = "-p project1 -r resource1 -a 100 -m test -u user1"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_charge_main, args.split())
        assert code == 0
        assert charges.count() == 1, "didn't create a charge"
        charge = charges.one()
        assert charge.allocation is allocation, "incorrect allocation: %r" % charge.allocation
        assert charge.amount == 200, "incorrect charge amount: %i" % charge.amount
        assert charge.comment == "test", "incorrect comment: %s" % charge.comment
        assert charge.user is user, "incorrect user on charge: %s" % charge.user
    
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
        args = "-p project1 -a 100 -m test -u user1"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_charge_main, args.split())
        assert code != 0
        assert not charges.count(), "created a charge"

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
        args = "-r resource1 -a 100 -m test -u user1"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_charge_main, args.split())
        assert code != 0
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
        args = "-p project1 -r resource1 -m test -u user1"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_charge_main, args.split())
        assert code != 0
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
        args = "-p project1 -r resource1 -a -100 -m test -u user1"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_charge_main, args.split())
        Session.remove()
        assert not charges.count(), "created a charge with negative amount: %s" % [(charge, charge.amount) for charge in charges]
        assert code != 0, code
    
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
        args = "-p project1 -r resource1 -a 100 -u user1"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_charge_main, args.split())
        assert code == 0
        assert charges.count() == 1, "didn't create a charge"
        charge = charges.one()
        assert charge.allocation is allocation, "incorrect allocation: %r" % charge.allocation
        assert charge.amount == 100, "incorrect charge amount: %i" % charge.amount
        assert charge.comment is None, "incorrect comment: %s" % charge.comment
        assert charge.user is user, "incorrect user on charge: %s" % charge.user
    
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
        args = "-p project1 -r resource1 -a 100 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_charge_main, args.split())
        assert code == 0
        assert charges.count() == 1, "didn't create a charge"
        charge = charges.one()
        assert charge.allocation is allocation, "incorrect allocation: %r" % charge.allocation
        assert charge.amount == 100, "incorrect charge amount: %i" % charge.amount
        assert charge.comment == "test", "incorrect comment: %s" % charge.comment
        assert charge.user is None, "incorrect user on charge: %s" % charge.user
    
    def test_non_admin (self):
        clusterbank.config.set("cbank", "admins", "")
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        charges = Session.query(Charge)
        assert not charges.count(), "started with existing charges"
        now = datetime.now()
        allocation = Allocation(project=project, resource=resource, amount=1000, start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        Session.save(allocation)
        Session.commit()
        args = "-p project1 -r resource1 -a 100 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_charge_main, args.split())
        Session.remove()
        assert not charges.count(), "created a charge without admin privileges"
        assert code != 0, code


class TestNewRefundMain (CbankTester):
    
    def test_exists_and_callable (self):
        assert hasattr(clusterbank.cbank.controllers, "new_refund_main"), "new_refund_main does not exist"
        assert callable(clusterbank.cbank.controllers.new_refund_main), "new_refund_main is not callable"
    
    def test_complete (self):
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource, amount=1000, start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "-c %s -a 50 -m test" % charge.id
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_refund_main, args.split())
        assert code == 0
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
        allocation = Allocation(project=project, resource=resource, amount=1000, start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "-c %s -a 50 -m test asdf" % charge.id
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_refund_main, args.split())
        assert not refunds.count()
        assert code != 0, code
    
    def test_with_defined_units (self):
        clusterbank.config.set("cbank", "unit_factor", "1/2")
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource, amount=1000, start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "-c %s -a 50 -m test" % charge.id
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_refund_main, args.split())
        assert code == 0
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
        allocation = Allocation(project=project, resource=resource, amount=1000, start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "-c %s -a 50" % charge.id
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_refund_main, args.split())
        assert code == 0
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
        allocation = Allocation(project=project, resource=resource, amount=1000, start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "-a 50 -m test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_refund_main, args.split())
        Session.remove()
        assert not refunds.count(), "created refund without charge"
        assert code != 0, code
    
    def test_without_amount (self):
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource, amount=1000, start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "-c %s -m test" % charge.id
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_refund_main, args.split())
        Session.remove()
        assert refunds.count() == 1, "incorrect refund count: %r" % [(refund, refund.amount) for refund in refunds]
        refund = refunds.one()
        assert refund.amount == 100
        assert code == 0, code
    
    def test_without_amount_with_existing_refund (self):
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource, amount=1000, start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        refund = Refund(charge=charge, amount=25)
        Session.save(allocation)
        Session.save(charge)
        Session.save(refund)
        Session.commit()
        args = "-c %s -m test" % charge.id
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_refund_main, args.split())
        Session.remove()
        assert refunds.count() == 2, "incorrect refund count: %r" % [(refund, refund.amount) for refund in refunds]
        assert sum(refund.amount for refund in refunds) == 100
        assert code == 0, code
    
    def test_non_admin (self):
        clusterbank.config.set("cbank", "admins", "")
        now = datetime.now()
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        refunds = Session.query(Refund)
        assert not refunds.count(), "started with existing refunds"
        allocation = Allocation(project=project, resource=resource, amount=1000, start=now-timedelta(days=1), expiration=now+timedelta(days=1))
        charge = Charge(allocation=allocation, amount=100)
        Session.save(allocation)
        Session.save(charge)
        Session.commit()
        args = "-c %s -a 50 -m test" % charge.id
        code, stdout, stderr = run(clusterbank.cbank.controllers.new_refund_main, args.split())
        Session.remove()
        assert not refunds.count(), "created a refund when not an admin"
        assert code != 0, code


class TestReportMain (CbankTester):
    
    def setup (self):
        CbankTester.setup(self)
        self._report_usage_main = \
            clusterbank.cbank.controllers.report_usage_main
        self._report_projects_main = \
            clusterbank.cbank.controllers.report_projects_main
        self._report_allocations_main = \
            clusterbank.cbank.controllers.report_allocations_main
        self._report_charges_main = \
            clusterbank.cbank.controllers.report_charges_main
    
    def teardown (self):
        CbankTester.teardown(self)
        clusterbank.cbank.controllers.report_usage_main = \
            self._report_usage_main
        clusterbank.cbank.controllers.report_projects_main = \
            self._report_projects_main
        clusterbank.cbank.controllers.report_allocations_main = \
            self._report_allocations_main
        clusterbank.cbank.controllers.report_charges_main = \
            self._report_charges_main
    
    def test_usage (self):
        args = "usage 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main usage", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        clusterbank.cbank.controllers.report_usage_main = fake
        run(clusterbank.cbank.controllers.report_main, args.split())
        assert self.fake_called
    
    def test_projects (self):
        args = "projects 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main projects", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        clusterbank.cbank.controllers.report_projects_main = fake
        run(clusterbank.cbank.controllers.report_main, args.split())
        assert self.fake_called
    
    def test_allocations (self):
        args = "allocations 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main allocations", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        clusterbank.cbank.controllers.report_allocations_main = fake
        run(clusterbank.cbank.controllers.report_main, args.split())
        assert self.fake_called
    
    def test_charges (self):
        args = "charges 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main charges", sys.argv
            assert sys.argv[1:] == args.split()[1:], sys.argv
        clusterbank.cbank.controllers.report_charges_main = fake
        run(clusterbank.cbank.controllers.report_main, args.split())
        assert self.fake_called
    
    def test_default (self):
        args = "1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main", sys.argv
            assert sys.argv[1:] == args.split(), sys.argv
        clusterbank.cbank.controllers.report_usage_main = fake
        run(clusterbank.cbank.controllers.report_main, args.split())
        assert self.fake_called
    
    def test_invalid (self):
        args = "invalid 1 2 3"
        def fake ():
            self.fake_called = True
            assert sys.argv[0] == "report_main", sys.argv
            assert sys.argv[1:] == args.split(), sys.argv
        clusterbank.cbank.controllers.report_usage_main = fake
        run(clusterbank.cbank.controllers.report_main, args.split())
        assert self.fake_called
    
    def test_exists_and_callable (self):
        assert hasattr(clusterbank.cbank.controllers, "report_main"), "report_main does not exist"
        assert callable(clusterbank.cbank.controllers.report_main), "report_main is not callable"
    
    def test_admin_reports_complete (self):
        self._run_all_reports()
    
    def test_member_reports_complete (self):
        clusterbank.config.set("cbank", "admins", "")
        for report in ("usage", "projects", "charges", "allocations"):
            run(clusterbank.cbank.controllers.report_main, [report])
    
    def _run_all_reports (self):
        for report in ("usage", "projects", "charges", "allocations"):
            run(clusterbank.cbank.controllers.report_main, [report])
