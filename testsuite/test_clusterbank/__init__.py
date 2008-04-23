from sqlalchemy import create_engine

import clusterbank
import clusterbank.model
from clusterbank.model import Project, Resource
import clusterbank.upstream.default as example

__all__ = ["test_model", "test_interfaces"]

def setup ():
    # bind the local database to memory
    clusterbank.model.metadata.bind = create_engine("sqlite:///:memory:")
    # create the upstream database
    example.metadata.bind = create_engine("sqlite:///:memory:")
    example.metadata.create_all()
    example.User(id=1, name="monty")
    example.Project(id=1, name="grail")
    example.Resource(id=1, name="spam")
    example.Session.commit()
    # attach an upstream module
    clusterbank.upstream = example

def teardown ():
    # clean up the userbase session
    example.Session.close()
    # destroy the upstream database
    example.metadata.drop_all()
    example.metadata.bind = None
    # detach the upstream module
    clusterbank.upstream = None
    # clean up the local session
    clusterbank.model.Session.close()
    # detach the local database from memory
    clusterbank.model.metadata.bind = None
