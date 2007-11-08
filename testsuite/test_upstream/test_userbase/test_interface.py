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
    
    def test_by_id (self):
        try:
            project = Project.by_id(2)
        except Project.DoesNotExist:
            pass
        else:
            assert not "did raise proper exception"
        
        project = Project.by_id(1)
        assert isinstance(project, Project)
        assert project.id == 1
    
    def test_by_name (self):
        try:
            project = Project.by_name("Spam")
        except Project.DoesNotExist:
            pass
        else:
            assert not "didn't raise proper exception"
        project = Project.by_name("Shrubbery")
        assert isinstance(project, Project)
        assert project.name == "Shrubbery"


class TestResource (UpstreamEntityTester):
    
    def setup (self):
        UpstreamEntityTester.setup(self)
        self.resource = Resource(id=1, name="Spam")
        Session.flush()
    
    def test_by_id (self):
        try:
            resource = Resource.by_id(2)
        except Resource.DoesNotExist:
            pass
        else:
            assert not "did raise proper exception"
        
        resource = Resource.by_id(1)
        assert isinstance(resource, Resource)
        assert resource.id == 1
    
    def test_by_name (self):
        try:
            resource = Resource.by_name("more spam")
        except Resource.DoesNotExist:
            pass
        else:
            assert not "didn't raise proper exception"
        resource = Resource.by_name("Spam")
        assert isinstance(resource, Resource)
        assert resource.name == "Spam"
