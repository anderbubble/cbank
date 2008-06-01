from nose.tools import raises

import clusterbank.model
import clusterbank.exceptions

class TestDBFunctions (object):
    
    def setup (self):
        """Create the tables before each test."""
        clusterbank.model.metadata.create_all()
    
    def teardown (self):
        """drop the database after each test."""
        clusterbank.model.Session.close()
        clusterbank.model.metadata.drop_all()

    def test_get_valid_user (self):
        user = clusterbank.model.user_by_name("monty")
        assert user.id == 1
    
    @raises(clusterbank.exceptions.NotFound)
    def test_get_invalid_user (self):
        user = clusterbank.model.user_by_name("doesnotexist")
    
    def test_get_valid_project (self):
        project = clusterbank.model.project_by_name("grail")
        assert project.id == 1
    
    @raises(clusterbank.exceptions.NotFound)
    def test_get_invalid_project (self):
        user = clusterbank.model.project_by_name("doesnotexist")
    
    def test_get_valid_resource (self):
        resource = clusterbank.model.resource_by_name("spam")
        assert resource.id == 1
    
    @raises(clusterbank.exceptions.NotFound)
    def test_get_invalid_resource (self):
        resource = clusterbank.model.resource_by_name("doesnotexist")
