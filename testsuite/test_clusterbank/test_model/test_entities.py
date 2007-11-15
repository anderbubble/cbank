from datetime import datetime, timedelta

import clusterbank.model
from clusterbank.model.entities import Project, Resource
from clusterbank.model.accounting import \
    Request, Allocation, Hold, Charge, Refund, CreditLimit

__all__ = [
    "TestUser", "TestProject", "TestResource",
]

class EntityTester (object):
    
    def setup (self):
        """Create the tables before each test."""
        clusterbank.model.metadata.create_all()
    
    def teardown (self):
        """drop the database after each test."""
        clusterbank.model.Session.close()
        clusterbank.model.metadata.drop_all()


class TestProject (EntityTester):
    
    def test_by_existing_name (self):
        project = Project.by_name("grail")
        assert project.id is not None
    
    def test_by_invalid_name (self):
        try:
            project = Project.by_name("doesnotexist")
        except Project.DoesNotExist:
            pass
        else:
            assert not "Got a project that should not exist."
    
    def test_name (self):
        project = Project.by_name("grail")
        assert project.name == "grail"


class TestResource (EntityTester):
    
    def test_by_existing_name (self):
        resource = Resource.by_name("spam")
        assert resource.id is not None
    
    def test_by_invalid_name (self):
        try:
            resource = Resource.by_name("doesnotexist")
        except Resource.DoesNotExist:
            pass
        else:
            assert not "Got a resource that should not exist."
    
    def test_name (self):
        resource = Resource.by_name("spam")
        assert resource.name == "spam"
