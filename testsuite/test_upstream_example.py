from nose.tools import raises

from sqlalchemy import create_engine

from upstream_example import metadata, Session, \
    NotFound, Project, Resource

__all__ = ["test_interface"]

def setup ():
    metadata.bind = create_engine("sqlite:///:memory:")

def teardown ():
    metadata.bind = None


class UpstreamEntityTester (object):
    
    def setup (self):
        metadata.create_all()
    
    def teardown (self):
        Session.close()
        metadata.drop_all()


class TestProject (UpstreamEntityTester):
    
    def setup (self):
        UpstreamEntityTester.setup(self)
        self.project = Project(id=1, name="Shrubbery")
        Session.flush()
    
    @raises(NotFound)
    def test_missing_by_id (self):
        project = Project.by_id(2)
    
    def test_existing_by_id (self):
        project = Project.by_id(1)
        assert isinstance(project, Project)
        assert project.id == 1
    
    @raises(NotFound)
    def test_missing_by_name (self):
        project = Project.by_name("Spam")
    
    def test_existing_by_name (self):
        project = Project.by_name("Shrubbery")
        assert isinstance(project, Project)
        assert project.name == "Shrubbery"


class TestResource (UpstreamEntityTester):
    
    def setup (self):
        UpstreamEntityTester.setup(self)
        self.resource = Resource(id=1, name="Spam")
        Session.flush()
    
    @raises(NotFound)
    def test_missing_by_id (self):
        resource = Resource.by_id(2)
    
    def test_existing_by_id (self):
        resource = Resource.by_id(1)
        assert isinstance(resource, Resource)
        assert resource.id == 1
    
    @raises(NotFound)
    def test_by_name (self):
        resource = Resource.by_name("more spam")
    
    def test_existing_by_name (self):
        resource = Resource.by_name("Spam")
        assert isinstance(resource, Resource)
        assert resource.name == "Spam"
