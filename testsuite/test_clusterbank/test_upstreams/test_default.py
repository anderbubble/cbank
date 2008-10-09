from nose.tools import raises

import clusterbank.upstreams.default
from clusterbank.upstreams.default import \
    get_owner_projects, get_member_projects, get_user_name, \
    get_user_id, get_resource_name, get_resource_id, \
    get_project_owners, get_project_name, \
    get_project_members, get_project_id, \
    User, Project, Resource


class UpstreamTester (object):
    
    def teardown (self):
        clusterbank.upstreams.default.users = []
        clusterbank.upstreams.default.projects = []
        clusterbank.upstreams.default.resources = []


class TestProject (UpstreamTester):
    
    def setup (self):
        project = Project(1, "Shrubbery")
        project.members = [User(1, "Monty")]
        project.owners = [User(2, "Python")]
        clusterbank.upstreams.default.projects = [project]
        clusterbank.upstreams.default.users = project.members + project.owners
    
    def test_name (self):
        assert get_project_name(1) == "Shrubbery"
        assert get_project_name(2) is None
    
    def test_id (self):
        assert get_project_id("Spam") is None
        assert get_project_id("Shrubbery") == 1
    
    def test_members (self):
        assert get_project_members(2) == []
        assert get_project_members(1) == [1], get_project_members(1)
    
    def test_owners (self):
        assert get_project_owners(2) == []
        assert get_project_owners(1) == [2], get_project_owners(1)


class TestResource (UpstreamTester):
    
    def setup (self):
        clusterbank.upstreams.default.resources = [
            Resource(1, "Spam"),
            Resource(3, "Life")]
            
    def test_id (self):
        assert get_resource_name(1) == "Spam"
        assert get_resource_name(2) is None
        assert get_resource_name(3) == "Life"
    
    def test_name (self):
        assert get_resource_id("Spam") == 1
        assert get_resource_id("more spam") is None
        assert get_resource_id("Life") == 3


class TestUser (UpstreamTester):
    
    def setup (self):
        user = User(1, "Monty")
        shrubbery = Project(1, "Shrubbery")
        shrubbery.members = [user]
        spam = Project(2, "Spam")
        spam.owners = [user]
        clusterbank.upstreams.default.projects = [shrubbery, spam]
        clusterbank.upstreams.default.users = [user]
    
    def test_name (self):
        assert get_user_name(2) is None
        assert get_user_name(1) == "Monty"
    
    def test_id (self):
        assert get_user_id("Python") is None
        assert get_user_id("Monty") == 1
    
    def test_projects (self):
        assert get_member_projects(2) == []
        assert get_member_projects(1) == [1]
    
    def test_projects_owned (self):
        assert get_owner_projects(2) == []
        assert get_owner_projects(1) == [2]

