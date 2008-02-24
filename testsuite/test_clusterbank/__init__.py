from sqlalchemy import create_engine

import clusterbank.model
from clusterbank.model import Project, Resource
from clusterbank import upstream
from clusterbank.upstream import userbase

__all__ = ["test_model", "test_interfaces"]

def setup ():
    # bind the local database to memory
    clusterbank.model.metadata.bind = create_engine("sqlite:///:memory:")
    # attach an upstream module (userbase, for now)
    upstream.Project = userbase.Project
    upstream.Resource = userbase.Resource
    upstream.NotFound = userbase.NotFound
    upstream.__all__ = list(set(upstream.__all__) | set(["Project", "Resource"]))
    # create the upstream database
    userbase.metadata.bind = create_engine("sqlite:///:memory:")
    userbase.metadata.create_all()
    # populate userbase
    userbase.Project(id=1, name="grail")
    userbase.Resource(id=1, name="spam")
    userbase.Session.commit()

def teardown ():
    # clean up the userbase session
    userbase.Session.close()
    # destroy the upstream database
    userbase.metadata.drop_all()
    userbase.metadata.bind = None
    # detach the upstream module
    del upstream.Project
    del upstream.Resource
    del upstream.NotFound
    upstream.__all__ = list(set(upstream.__all__).difference(set(["Project", "Resource"])))
    # clean up the local session
    clusterbank.model.Session.close()
    # detach the local database from memory
    clusterbank.model.metadata.bind = None
