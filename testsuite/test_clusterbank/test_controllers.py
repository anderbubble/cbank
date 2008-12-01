from nose.tools import raises, assert_equal

from sqlalchemy import create_engine

from clusterbank import config
from clusterbank.model import metadata, upstream, Job
from clusterbank.upstreams import default as upstream_
from clusterbank.upstreams.default import User, Project, Resource
import clusterbank.controllers
from clusterbank.controllers import (Session, user, project, user_by_name,
    project_by_name, resource, resource_by_name, job_from_pbs)
from clusterbank.exceptions import NotFound
from clusterbank.model import metadata, Allocation, Charge

from datetime import datetime, timedelta


def setup ():
    metadata.bind = create_engine("sqlite:///:memory:")
    upstream.use = upstream_
    upstream_.users = [User(1, "monty")]
    upstream_.projects = [Project(1, "grail")]
    upstream_.resources = [Resource(1, "spam")]


def teardown ():
    upstream_.users = []
    upstream_.projects = []
    upstream_.resources = []
    upstream.use = None
    metadata.bind = None


def assert_ident (obj1, obj2):
    assert obj1 is obj2, "%r is not %r" % (obj1, obj2)


class FakeDateTime (object):

    def __init__ (self, now):
        self._now = now

    def __call__ (self, *args):
        return datetime(*args)
    
    def __getattr__ (self, attribute):
        return getattr(datetime, attribute)

    def now (self):
        return self._now


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

    def setup (self):
        """Create the tables before each test."""
        metadata.create_all()
        self._datetime = clusterbank.model.entities.datetime
        clusterbank.model.entities.datetime = \
            FakeDateTime(datetime(2000, 1, 1))
    
    def teardown (self):
        """drop the database after each test."""
        Session.remove()
        metadata.drop_all()
        clusterbank.model.entities.datetime = self._datetime
    
    def test_job_from_pbs_q (self):
        entry = "04/18/2008 02:10:12;Q;692009.jmayor5.lcrc.anl.gov;queue=shared"
        job_ = job_from_pbs(entry)
        assert job_.id == "692009.jmayor5.lcrc.anl.gov"
        assert job_.queue == "shared"
    
    def test_job_from_pbs_s (self):
        entry = "04/18/2008 02:10:12;S;692009.jmayor5.lcrc.anl.gov;user=monty group=agroup account=grail jobname=myjob queue=shared ctime=1208502612 qtime=1208502612 etime=1208502612 start=1208502612 exec_host=j340/0+j341/0+j342/0+j343/0+j344/0+j345/0+j346/0+j347/0 Resource_List.ncpus=8 Resource_List.neednodes=8 Resource_List.nodect=8 Resource_List.nodes=8 Resource_List.walltime=05:00:00"
        job_ = job_from_pbs(entry)
        assert_equal(job_.id, "692009.jmayor5.lcrc.anl.gov")
        assert_ident(job_.user, user("monty"))
        assert_equal(job_.group, "agroup")
        assert_ident(job_.account, project("grail"))
        assert_equal(job_.name, "myjob")
        assert_equal(job_.queue, "shared")
        assert_equal(job_.ctime, datetime(2008, 4, 18, 2, 10, 12))
        assert_equal(job_.qtime, datetime(2008, 4, 18, 2, 10, 12))
        assert_equal(job_.etime, datetime(2008, 4, 18, 2, 10, 12))
        assert_equal(job_.start, datetime(2008, 4, 18, 2, 10, 12))
        assert_equal(job_.exec_host,
            "j340/0+j341/0+j342/0+j343/0+j344/0+j345/0+j346/0+j347/0")
        assert_equal(job_.resource_list, {'ncpus':8, 'neednodes':8,
            'nodect':8, 'nodes':8, 'walltime':timedelta(hours=5)})
    
    def test_job_from_pbs_e (self):
        entry = "04/18/2008 03:35:28;E;691908.jmayor5.lcrc.anl.gov;user=monty group=agroup account=grail jobname=myjob queue=pri4 ctime=1208378066 qtime=1208378066 etime=1208378066 start=1208378066 exec_host=j75/0+j76/0+j77/0+j78/0+j79/0+j86/0+j87/0+j88/0+j89/0+j90/0+j91/0+j93/0+j94/0+j100/0+j101/0+j102/0+j103/0+j104/0+j105/0+j106/0+j107/0+j108/0+j109/0+j110/0 Resource_List.ncpus=24 Resource_List.neednodes=24 Resource_List.nodect=24 Resource_List.nodes=24 Resource_List.walltime=36:00:00 session=23061 end=1208507728 Exit_status=265 resources_used.cpupercent=0 resources_used.cput=00:00:08 resources_used.mem=41684kb resources_used.ncpus=24 resources_used.vmem=95988kb resources_used.walltime=36:00:47"
        job_ = job_from_pbs(entry)
        assert_equal(job_.id, "691908.jmayor5.lcrc.anl.gov")
        assert_ident(job_.user, user("monty"))
        assert_equal(job_.group, "agroup")
        assert_ident(job_.account, project("grail"))
        assert_equal(job_.name, "myjob")
        assert_equal(job_.queue, "pri4")
        assert_equal(job_.ctime, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.qtime, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.etime, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.start, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.exec_host, "j75/0+j76/0+j77/0+j78/0+j79/0+j86/0+j87/0+j88/0+j89/0+j90/0+j91/0+j93/0+j94/0+j100/0+j101/0+j102/0+j103/0+j104/0+j105/0+j106/0+j107/0+j108/0+j109/0+j110/0")
        assert_equal(job_.resource_list, {'ncpus':24, 'neednodes':24,
            'nodect':24, 'nodes':24, 'walltime':timedelta(hours=36)})
        assert_equal(job_.resources_used, {
            'walltime':timedelta(hours=36, seconds=47),
            'cput':timedelta(seconds=8), 'cpupercent':0, 'vmem':"95988kb",
            'ncpus':24, 'mem':"41684kb"})
        assert_equal(job_.session, 23061)
        assert_equal(job_.end, datetime(2008, 4, 18, 3, 35, 28))
        assert_equal(job_.exit_status, 265)
    
    def test_job_from_pbs_e_with_malformed_message (self):
        entry = "04/18/2008 03:35:28;E;691908.jmayor5.lcrc.anl.gov;user=monty group=agroup account=grail jobname=myjob queue=pri4 ctime=1208378066 invalid=value with spaces qtime=1208378066 etime=1208378066 start=1208378066 exec_host=j75/0+j76/0+j77/0+j78/0+j79/0+j86/0+j87/0+j88/0+j89/0+j90/0+j91/0+j93/0+j94/0+j100/0+j101/0+j102/0+j103/0+j104/0+j105/0+j106/0+j107/0+j108/0+j109/0+j110/0 Resource_List.ncpus=24 Resource_List.neednodes=24 Resource_List.nodect=24 Resource_List.nodes=24 Resource_List.walltime=36:00:00 session=23061 end=1208507728 Exit_status=265 resources_used.cpupercent=0 resources_used.cput=00:00:08 resources_used.mem=41684kb resources_used.ncpus=24 resources_used.vmem=95988kb resources_used.walltime=36:00:47"
        job_ = job_from_pbs(entry)
        assert_equal(job_.id, "691908.jmayor5.lcrc.anl.gov")
        assert_ident(job_.user, user("monty"))
        assert_equal(job_.group, "agroup")
        assert_ident(job_.account, project("grail"))
        assert_equal(job_.name, "myjob")
        assert_equal(job_.queue, "pri4")
        assert_equal(job_.ctime, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.qtime, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.etime, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.start, datetime(2008, 4, 16, 15, 34, 26))
        assert_equal(job_.exec_host, "j75/0+j76/0+j77/0+j78/0+j79/0+j86/0+j87/0+j88/0+j89/0+j90/0+j91/0+j93/0+j94/0+j100/0+j101/0+j102/0+j103/0+j104/0+j105/0+j106/0+j107/0+j108/0+j109/0+j110/0")
        assert_equal(job_.resource_list, {'ncpus':24, 'neednodes':24,
            'nodect':24, 'nodes':24, 'walltime':timedelta(hours=36)})
        assert_equal(job_.resources_used, {
            'walltime':timedelta(hours=36, seconds=47),
            'cput':timedelta(seconds=8), 'cpupercent':0, 'vmem':"95988kb",
            'ncpus':24, 'mem':"41684kb"})
        assert_equal(job_.session, 23061)
        assert_equal(job_.end, datetime(2008, 4, 18, 3, 35, 28))
        assert_equal(job_.exit_status, 265)
    
    def test_job_from_pbs_invalid_account (self):
        entry = "04/18/2008 03:35:28;E;691908.jmayor5.lcrc.anl.gov;account=invalid"
        job_ = job_from_pbs(entry)
        assert_ident(job_.account, None)
    
    def test_job_from_pbs_invalid_user (self):
        entry = "04/18/2008 03:35:28;E;691908.jmayor5.lcrc.anl.gov;user=invalid"
        job_ = job_from_pbs(entry)
        assert_ident(job_.user, None)
 
