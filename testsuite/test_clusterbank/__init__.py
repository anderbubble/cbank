from sqlalchemy import create_engine

import clusterbank
import clusterbank.model
from clusterbank.model import Project, Resource
import clusterbank.upstreams.default as upstream

__all__ = ["test_model", "test_interfaces"]

def setup ():
    # bind the local database to memory
    clusterbank.model.metadata.bind = create_engine("sqlite:///:memory:")
    # create the upstream database
    upstream.metadata.bind = create_engine("sqlite:///:memory:")
    upstream.metadata.create_all()
    upstream.User(id=1, name="monty")
    upstream.Project(id=1, name="grail")
    upstream.Resource(id=1, name="spam")
    upstream.Session.commit()
    # attach an upstream module
    clusterbank.upstream = upstream

def teardown ():
    # clean up the userbase session
    upstream.Session.close()
    # destroy the upstream database
    upstream.metadata.drop_all()
    upstream.metadata.bind = None
    # detach the upstream module
    clusterbank.upstream = None
    # clean up the local session
    clusterbank.model.Session.close()
    # detach the local database from memory
    clusterbank.model.metadata.bind = None
