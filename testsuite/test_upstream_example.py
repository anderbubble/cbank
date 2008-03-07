from nose.tools import raises

from sqlalchemy import create_engine

import upstream_example as example
from upstream_example import metadata, Session, User, Project, Resource, \
    get_user_id, get_user_name, \
    get_project_id, get_project_name, \
    get_resource_id, get_resource_name


class UpstreamEntityTester (object):
    
    def setup (self):
        metadata.bind = create_engine("sqlite:///:memory:")
        metadata.create_all()
    
    def teardown (self):
        Session.close()
        metadata.drop_all()
        metadata.bind = None


class TestProject (UpstreamEntityTester):
    
    def setup (self):
        UpstreamEntityTester.setup(self)
        self.project = Project(id=1, name="Shrubbery")
        Session.flush()
    
    def test_missing_by_id (self):
        assert get_project_name(2) is None
    
    def test_existing_by_id (self):
        assert get_project_name(1) == "Shrubbery"
    
    def test_missing_by_name (self):
        assert get_project_id("Spam") is None
    
    def test_existing_by_name (self):
        assert get_project_id("Shrubbery") == 1


class TestResource (UpstreamEntityTester):
    
    def setup (self):
        UpstreamEntityTester.setup(self)
        self.resource = Resource(id=1, name="Spam")
        Session.flush()
    
    def test_missing_by_id (self):
        assert get_resource_name(2) is None
    
    def test_existing_by_id (self):
        assert get_resource_name(1) == "Spam"
    
    def test_by_name (self):
        assert get_resource_id("more spam") is None
    
    def test_existing_by_name (self):
        assert get_resource_id("Spam") == 1


class TestUser (UpstreamEntityTester):
    
    def setup (self):
        UpstreamEntityTester.setup(self)
        self.user = User(id=1, name="Monty")
        Session.flush()
    
    def test_missing_by_id (self):
        assert get_user_name(2) is None
    
    def test_existing_by_id (self):
        assert get_user_name(1) == "Monty"
    
    def test_by_name (self):
        assert get_user_id("Python") is None
    
    def test_existing_by_name (self):
        assert get_user_id("Monty") == 1
