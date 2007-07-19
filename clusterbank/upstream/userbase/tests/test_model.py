import clusterbank.upstream.userbase.model as model
from django.core.management import setup_environ
import settings # Assumed to be in the same directory.

setup_environ(settings)

TEST_USER = dict(
    id = 258,
    name = "abate")

INVALID_USER = dict(
    id = 0,
    name = "")

TEST_PROJECT = dict(
    id = 1,
    name = "grp-petrol")

INVALID_PROJECT = dict(
    id = 0,
    name = "")

TEST_RESOURCE = dict(
    id = 18,
    name = "lcrc")

INVALID_RESOURCE = dict(
    id = 0,
    name = "")

class TestUser (object):
    
    def test_interface_by_id (self):
        user = model.User.by_id(TEST_USER['id'])
        assert isinstance(user, model.User)
    
    def test_interface_by_name (self):
        user = model.User.by_name(TEST_USER['name'])
        assert isinstance(user, model.User)
    
    def test_unity_by_name_and_id (self):
        user1 = model.User.by_id(TEST_USER['id'])
        user2 = model.User.by_name(TEST_USER['name'])
        assert user1 is user2
    
    def test_interface_id (self):
        user = model.User.by_id(TEST_USER['id'])
        assert user.id == TEST_USER['id']
    
    def test_interface_name (self):
        user = model.User.by_name(TEST_USER['name'])
        assert user.name == TEST_USER['name']
    
    def test_interface_projects (self):
        user = model.User.by_id(TEST_USER['id'])
        try:
            projects = iter(user.projects)
        except TypeError:
            iterable = False
        else:
            iterable = True
        assert iterable
        for project in projects:
            assert isinstance(project, model.Project)
    
    def test_invalid_id (self):
        try:
            user = model.User.by_id(INVALID_USER['id'])
        except Exception, e:
            assert isinstance(e, model.DoesNotExist)
            user = None
        else:
            e = None
        assert e is not None
    
    def test_invalid_name (self):
        try:
            user = model.User.by_name(INVALID_USER['name'])
        except Exception, e:
            assert isinstance(e, model.DoesNotExist)
            user = None
        else:
            e = None
        assert e is not None


class TestProject (object):
    
    def test_interface_by_id (self):
        project = model.Project.by_id(TEST_PROJECT['id'])
        assert isinstance(project, model.Project)
    
    def test_interface_by_name (self):
        project = model.Project.by_name(TEST_PROJECT['name'])
        assert isinstance(project, model.Project)
    
    def test_unity_by_name_and_id (self):
        project1 = model.Project.by_id(TEST_PROJECT['id'])
        project2 = model.Project.by_name(TEST_PROJECT['name'])
        assert project1 is project2
    
    def test_interface_id (self):
        project = model.Project.by_id(TEST_PROJECT['id'])
        assert project.id == TEST_PROJECT['id']
    
    def test_interface_name (self):
        project = model.Project.by_name(TEST_PROJECT['name'])
        assert project.name == TEST_PROJECT['name']
    
    def test_interface_users (self):
        project = model.Project.by_id(TEST_PROJECT['id'])
        try:
            users = iter(project.users)
        except TypeError:
            iterable = False
        else:
            iterable = True
        assert iterable
        for user in users:
            assert isinstance(user, model.User)
    
    def test_invalid_id (self):
        try:
            project = model.Project.by_id(INVALID_PROJECT['id'])
        except Exception, e:
            assert isinstance(e, model.DoesNotExist)
            project = None
        else:
            e is None
        assert e is not None
    
    def test_invalid_name (self):
        try:
            project = model.Project.by_name(INVALID_PROJECT['name'])
        except Exception, e:
            assert isinstance(e, model.DoesNotExist)
            project = None
        else:
            e = None
        assert e is not None


class TestResource (object):
    
    def test_interface_by_id (self):
        resource = model.Resource.by_id(TEST_RESOURCE['id'])
        assert isinstance(resource, model.Resource)
    
    def test_interface_by_name (self):
        resource = model.Resource.by_name(TEST_RESOURCE['name'])
        assert isinstance(resource, model.Resource)
    
    def test_interface_unity_by_name_and_id (self):
        resource1 = model.Resource.by_id(TEST_RESOURCE['id'])
        resource2 = model.Resource.by_name(TEST_RESOURCE['name'])
        assert resource1 is resource2
    
    def test_interface_id (self):
        resource = model.Resource.by_id(TEST_RESOURCE['id'])
        assert resource.id == TEST_RESOURCE['id']
    
    def test_interface_name (self):
        resource = model.Resource.by_name(TEST_RESOURCE['name'])
        assert resource.name == TEST_RESOURCE['name']
    
    def test_invalid_id (self):
        try:
            resource = model.Resource.by_id(INVALID_RESOURCE['id'])
        except Exception, e:
            assert isinstance(e, model.DoesNotExist)
            resource = None
        else:
            e = None
        assert e is not None
    
    def test_invalid_name (self):
        try:
            resource = model.Resource.by_name(INVALID_RESOURCE['name'])
        except Exception, e:
            assert isinstance(e, model.DoesNotExist)
            resource = None
        else:
            e = None
        assert e is not None
