from nose.tools import raises

from sqlalchemy import create_engine

from clusterbank.upstream.default import *
from clusterbank.upstream.default import Session, metadata, User, Project, Resource


class UpstreamEntityTester (object):
    
    def setup (self):
        metadata.bind = create_engine("sqlite:///:memory:", echo=True)
        metadata.create_all()
    
    def teardown (self):
        Session.close()
        metadata.drop_all()
        metadata.bind = None


class TestProject (UpstreamEntityTester):
    
    def setup (self):
        UpstreamEntityTester.setup(self)
        self.project = Project(id=1, name="Shrubbery",
            members=[User(id=1, name="Monty")],
            owners=[User(id=2, name="Python")])
        Session.flush()
    
    def test_missing_name (self):
        assert get_project_name(2) is None
    
    def test_existing_name (self):
        assert get_project_name(1) == "Shrubbery"
    
    def test_missing_id (self):
        assert get_project_id("Spam") is None
    
    def test_existing_id (self):
        assert get_project_id("Shrubbery") == 1
    
    def test_missing_members (self):
        assert get_project_members(2) == []
    
    def test_existing_members (self):
        assert get_project_members(1) == [1], get_project_members(1)
    
    def test_missing_owners (self):
        assert get_project_owners(2) == []
    
    def test_existing_owners (self):
        assert get_project_owners(1) == [2], get_project_owners(1)


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
        self.user = User(id=1, name="Monty",
            projects=[Project(id=1, name="Shrubbery")],
            projects_owned=[Project(id=2, name="Spam")])
        Session.flush()
    
    def test_missing_name (self):
        assert get_user_name(2) is None
    
    def test_existing_name (self):
        assert get_user_name(1) == "Monty"
    
    def test_missing_id (self):
        assert get_user_id("Python") is None
    
    def test_existing_id (self):
        assert get_user_id("Monty") == 1
    
    def test_missing_projects (self):
        assert get_member_projects(2) == []
    
    def test_existing_projects (self):
        assert get_member_projects(1) == [1]
    
    def test_missing_projects_owned (self):
        assert get_owner_projects(2) == []
    
    def test_existing_projects_owned (self):
        assert get_owner_projects(1) == [2]
