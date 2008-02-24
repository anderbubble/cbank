from nose.tools import raises

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
    
    @raises(Project.DoesNotExist)
    def test_by_invalid_name (self):
        project = Project.by_name("doesnotexist")
    
    def test_name (self):
        project = Project.by_name("grail")
        assert project.name == "grail"


class TestResource (EntityTester):
    
    def test_by_existing_name (self):
        resource = Resource.by_name("spam")
        assert resource.id is not None
    
    @raises(Resource.DoesNotExist)
    def test_by_invalid_name (self):
        resource = Resource.by_name("doesnotexist")
    
    def test_name (self):
        resource = Resource.by_name("spam")
        assert resource.name == "spam"
