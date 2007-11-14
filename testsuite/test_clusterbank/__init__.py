from sqlalchemy import create_engine

import clusterbank.model
from clusterbank.model import Project, Resource
from clusterbank import upstream
from clusterbank.upstream import userbase

__all__ = ["test_models", "test_interfaces"]

def setup ():
    clusterbank.model.metadata.bind = create_engine("sqlite:///:memory:")
    clusterbank.model.metadata.create_all()
    upstream.Project = userbase.Project
    upstream.Resource = userbase.Resource
    userbase.metadata.bind = create_engine("sqlite:///:memory:")
    userbase.metadata.create_all()
    
    grail = userbase.Project(id=1, name="grail")
    meaning = userbase.Project(id=2, name="meaning")
    userbase.Project(id=3, name="circus")
    userbase.Project(id=4, name="brian")
    userbase.Resource(id=1, name="spam")
    userbase.Session.flush()

def teardown ():
    clusterbank.model.Session.close()
    clusterbank.model.metadata.drop_all()
    clusterbank.model.metadata.bind = None
    userbase.Session.close()
    userbase.metadata.drop_all()
    userbase.metadata.bind = None
    del upstream.Project
    del upstream.Resource
