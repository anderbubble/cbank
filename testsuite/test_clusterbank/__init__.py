from sqlalchemy import create_engine

import clusterbank.model
from clusterbank.model import Project, Resource
from clusterbank import upstream
import clusterbank.upstream.example as example

__all__ = ["test_model", "test_interfaces"]

def setup ():
    # bind the local database to memory
    clusterbank.model.metadata.bind = create_engine("sqlite:///:memory:")
    # attach an upstream module
    upstream.Project = example.Project
    upstream.Resource = example.Resource
    upstream.NotFound = example.NotFound
    upstream.__all__ = list(set(upstream.__all__) | set(["Project", "Resource", "NotFound"]))
    # create the upstream database
    example.metadata.bind = create_engine("sqlite:///:memory:")
    example.metadata.create_all()
    # populate upstream
    example.Project(id=1, name="grail")
    example.Resource(id=1, name="spam")
    example.Session.commit()

def teardown ():
    # clean up the userbase session
    example.Session.close()
    # destroy the upstream database
    example.metadata.drop_all()
    example.metadata.bind = None
    # detach the upstream module
    del upstream.Project
    del upstream.Resource
    del upstream.NotFound
    upstream.__all__ = list(set(upstream.__all__).difference(set(["Project", "Resource", "NotFound"])))
    # clean up the local session
    clusterbank.model.Session.close()
    # detach the local database from memory
    clusterbank.model.metadata.bind = None
