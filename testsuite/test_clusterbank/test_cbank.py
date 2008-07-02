from nose.tools import raises

import sys
import pwd
import os

from datetime import datetime
from StringIO import StringIO
from sqlalchemy import create_engine

from clusterbank.model import Session, Allocation, project_by_name, resource_by_name
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
    current_user = get_current_username()
    upstream.Session.save(upstream.User(id=1, name=current_user))
    upstream.Session.commit()

def teardown ():
    upstream.metadata.drop_all()
    upstream.metadata.bind = None
    clusterbank.model.upstream.use = None
    clusterbank.model.metadata.bind = None

def run (func, args):
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


class TestAllocationMain (object):
    
    def setup (self):
        clusterbank.model.metadata.create_all()
        current_user = get_current_username()
        clusterbank.config.add_section("cbank")
        clusterbank.config.set("cbank", "admins", current_user)
    
    def teardown (self):
        clusterbank.model.metadata.drop_all()
        Session.close()
        clusterbank.config.remove_section("cbank")
    
    def test_exists_and_callable (self):
        assert hasattr(clusterbank.cbank.controllers, "allocation_main"), "allocation_main does not exist"
        assert callable(clusterbank.cbank.controllers.allocation_main), "allocation_main is not callable"
    
    def test_complete (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a 1000 -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert query.count() == 1, "didn't create an allocation"
        allocation = query.one()
        assert allocation.start == datetime(2008, 1, 1), allocation.start
        assert allocation.expiration == datetime(2009, 1, 1), allocation.expiration
        assert allocation.amount == 1000, allocation.amount
        assert allocation.comment == "test", allocation.comment
        assert code == 0, code
    
    def test_with_bad_start (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-p project1 -r resource1 -s bad_start -e 2009-01-01 -a 1000 -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert query.count() == 0, "created an allocation with bad start"
        assert code != 0, code
    
    def test_with_bad_end (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e bad_end -a 1000 -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert query.count() == 0, "created an allocation with bad end"
        assert code != 0, code
    
    def test_with_bad_amount (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a bad_amount -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert query.count() == 0, "created an allocation with bad amount"
        assert code != 0, code

    def test_with_negative_amount (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a -1000 -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert query.count() == 0, "created an allocation with negative amount"
        assert code != 0, code
    
    def test_without_comment (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a 1000"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
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
        assert query.count() == 0, "started with existing allocations"
        args = "-r resource1 -s 2008-01-01 -e 2009-01-01 -a 1000 -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert not query.count(), "created allocation without project: %s" % new_allocations
        assert code != 0, code
    
    def test_without_amount (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert query.count() == 0, "created allocation without amount"
        assert code != 0, code
    
    def test_without_start (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-p project1 -r resource1 -e 2009-01-01 -a 1000 -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert query.count() == 0, "created allocation without start"
        sys.stdout.write(stderr.read())
        assert code != 0, code
    
    def test_without_expiration (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -a 1000 -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert query.count() == 0, "created allocation without expiration"
        sys.stdout.write(stderr.read())
        assert code != 0, code

    def test_without_project_or_resource (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-s 2008-01-01 -e 2009-01-01 -a 1000 -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert not query.count(), "created allocation without project or resource: %s" % new_allocations
        assert code != 0, code
    
    def test_without_resource (self):
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-p project1 -s 2008-01-01 -e 2009-01-01 -a 1000 -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert not query.count(), "created allocation without resource: %s" % new_allocations
        assert code != 0, code

    def test_non_admin (self):
        clusterbank.config.set("cbank", "admins", "")
        project = project_by_name("project1")
        resource = resource_by_name("resource1")
        query = Session.query(Allocation).filter_by(project=project, resource=resource)
        assert query.count() == 0, "started with existing allocations"
        args = "-p project1 -r resource1 -s 2008-01-01 -e 2009-01-01 -a 1000 -c test"
        code, stdout, stderr = run(clusterbank.cbank.controllers.allocation_main, args.split())
        Session.close()
        assert not query.count(), "created allocation when not admin: %s" % new_allocations
        assert code != 0

