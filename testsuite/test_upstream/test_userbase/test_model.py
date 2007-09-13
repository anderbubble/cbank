from sqlalchemy import create_engine
from clusterbank.upstream.userbase.model import \
    metadata, context, \
    User, Project, Resource

def setup ():
    metadata.bind = create_engine("sqlite:///:memory:")

def teardown ():
    metadata.bind = None


class UpstreamEntityTester (object):
    
    LABEL = "entity"
    
    def setup (self):
        metadata.create_all()
        self.entity = self.Entity(
            id = self.ID,
            name = self.NAME,
        )
        context.current.flush()
    
    def teardown (self):
        context.current.clear()
        metadata.drop_all()
    
    def test_str (self):
        assert str(self.entity) == self.entity.name
        self.entity.name = None
        assert str(self.entity) == "?"
    
    def test_repr (self):
        assert repr(self.entity) == "<%s %i>" % (self.entity.__class__.__name__, self.entity.id)
        self.entity.id = None
        assert repr(self.entity) == "<%s ?>" % self.entity.__class__.__name__
    
    def test_by_id (self):
        try:
            entity = self.Entity.by_id(self.INVALID_ID)
        except self.Entity.DoesNotExist, e:
            assert str(e) == '%s %i does not exist' % (self.LABEL, self.INVALID_ID)
        else:
            assert not "by_id(INVALID_ID) didn't raise proper exception."
        
        entity = self.Entity.by_id(self.ID)
        assert isinstance(entity, self.Entity)
        assert entity.id == self.ID
    
    def test_by_name (self):
        try:
            entity = self.Entity.by_name(self.INVALID_NAME)
        except self.Entity.DoesNotExist, e:
            assert str(e) == '%s "%s" does not exist' % (self.LABEL, self.INVALID_NAME)
        else:
            assert not "by_name(INVALID_NAME) didn't raise proper exception."
        
        entity = self.Entity.by_name(self.NAME)
        assert isinstance(entity, self.Entity)
        assert entity.name == self.NAME
    
    def test_object_identity (self):
        entity = self.Entity.by_id(self.ID)
        assert entity is self.Entity.by_name(entity.name)
        
        entity = self.Entity.by_name(self.NAME)
        assert entity is self.Entity.by_id(entity.id)


class TestUser (UpstreamEntityTester):
    
    Entity = User
    LABEL = "user"
    
    ID = 1
    NAME = "user1"
    
    INVALID_ID = 0
    INVALID_NAME = ""
    
    def setup (self):
        UpstreamEntityTester.setup(self)
        project = Project(id=1, name="project1")
        self.entity.projects.append(project)
        context.current.flush()
    
    def test_projects (self):
        user = self.Entity.by_id(self.ID)
        assert len(user.projects) == 1
        for project in user.projects:
            assert isinstance(project, Project)


class TestProject (UpstreamEntityTester):
    
    Entity = Project
    LABEL = "project"
    
    ID = 1
    NAME = "project1"
    
    INVALID_ID = 0
    INVALID_NAME = ""
    
    def setup (self):
        UpstreamEntityTester.setup(self)
        user = User(id=1, name="user1")
        self.entity.users.append(user)
        context.current.flush()
    
    def test_users (self):
        project = self.Entity.by_id(self.ID)
        assert len(project.users) == 1
        for user in project.users:
            assert isinstance(user, User)


class TestResource (UpstreamEntityTester):
    
    Entity = Resource
    LABEL = "resource"
    
    ID = 1
    NAME = "resource1"
    
    INVALID_ID = 0
    INVALID_NAME = ""
