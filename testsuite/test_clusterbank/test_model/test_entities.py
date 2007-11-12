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
    
    def setup (self):
        EntityTester.setup(self)
        self.project = Project.by_name("grail")
    
    def test_name (self):
        assert self.project.name == "grail"


class TestResource (EntityTester):
    
    def setup (self):
        EntityTester.setup(self)
        self.resource = Resource.by_name("spam")
    
    def test_name (self):
        assert self.resource.name == "spam"
