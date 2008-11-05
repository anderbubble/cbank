from nose.tools import raises

from clusterbank.model import Session, user_by_name, project_by_name, \
    resource_by_name, job
from clusterbank.exceptions import NotFound
from clusterbank.model.database import metadata


class TestDBFunctions (object):
    
    def setup (self):
        """Create the tables before each test."""
        metadata.create_all()
    
    def teardown (self):
        """drop the database after each test."""
        Session.remove()
        metadata.drop_all()

    def test_get_valid_user (self):
        user = user_by_name("monty")
        assert user.id == 1
    
    @raises(NotFound)
    def test_get_invalid_user (self):
        user = user_by_name("doesnotexist")
    
    def test_get_valid_project (self):
        project = project_by_name("grail")
        assert project.id == 1
    
    @raises(NotFound)
    def test_get_invalid_project (self):
        user = project_by_name("doesnotexist")
    
    def test_get_valid_resource (self):
        resource = resource_by_name("spam")
        assert resource.id == 1
    
    @raises(NotFound)
    def test_get_invalid_resource (self):
        resource = resource_by_name("doesnotexist")


class TestJobFunctions (object):
    
    def test_job_q (self):
        entry = "04/18/2008 02:10:12;Q;692009.jmayor5.lcrc.anl.gov;queue=shared"
        job_ = job(entry)
        assert job_.id == "692009.jmayor5.lcrc.anl.gov"
        assert job_.queue == "shared"

