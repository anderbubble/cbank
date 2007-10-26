from clusterbank.upstream.userbase import metadata, Session, User, Project, Resource
from clusterbank.upstream.userbase import Session


class UpstreamEntityTester (object):
    
    def setup (self):
        metadata.create_all()
    
    def teardown (self):
        Session.close()
        metadata.drop_all()


class TestUser (UpstreamEntityTester):
    
    def setup (self):
        UpstreamEntityTester.setup(self)
        self.user = User(id=1, name="Monty")
        project = Project(id=1, name="shrubbery")
        self.user.projects.append(project)
        Session.flush()
    
    def test_by_id (self):
        try:
            user = User.by_id(2)
        except User.DoesNotExist:
            pass
        else:
            assert not "did raise proper exception"
        
        user = User.by_id(1)
        assert isinstance(user, User)
        assert user.id == 1
    
    def test_by_name (self):
        try:
            user = User.by_name("Python")
        except User.DoesNotExist:
            pass
        else:
            assert not "didn't raise proper exception"
        
        user = User.by_name("Monty")
        assert isinstance(user, User)
        assert user.name == "Monty"
    
    def test_projects (self):
        assert len(self.user.projects) > 0
        for project in self.user.projects:
            assert isinstance(project, Project)


class TestProject (UpstreamEntityTester):
    
    def setup (self):
        UpstreamEntityTester.setup(self)
        user = User(id=1, name="Monty")
        self.project = Project(id=1, name="Shrubbery")
        self.project.users.append(user)
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
    
    def test_users (self):
        assert len(self.project.users) > 0
        for user in self.project.users:
            assert isinstance(user, User)


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
