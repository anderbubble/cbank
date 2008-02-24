from nose.tools import raises

from clusterbank.upstream.userbase import metadata, Session, Project, Resource
from clusterbank.upstream.userbase import Session


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
    
    @raises(Project.DoesNotExist)
    def test_missing_by_id (self):
        project = Project.by_id(2)
    
    def test_existing_by_id (self):
        project = Project.by_id(1)
        assert isinstance(project, Project)
        assert project.id == 1
    
    @raises(Project.DoesNotExist)
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
    
    @raises(Resource.DoesNotExist)
    def test_missing_by_id (self):
        resource = Resource.by_id(2)
    
    def test_existing_by_id (self):
        resource = Resource.by_id(1)
        assert isinstance(resource, Resource)
        assert resource.id == 1
    
    @raises(Resource.DoesNotExist)
    def test_by_name (self):
        resource = Resource.by_name("more spam")
    
    def test_existing_by_name (self):
        resource = Resource.by_name("Spam")
        assert isinstance(resource, Resource)
        assert resource.name == "Spam"
