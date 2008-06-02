from sqlalchemy import create_engine

import clusterbank.model
from clusterbank.upstreams import default as upstream

__all__ = ["test_base", "test_entities", "test_accounting"]

def setup ():
    clusterbank.model.metadata.bind = create_engine("sqlite:///:memory:")
    upstream.metadata.bind = create_engine("sqlite:///:memory:", echo=True)
    upstream.metadata.create_all()
    populate_upstream()
    clusterbank.model.upstream.use = upstream

def populate_upstream ():
    upstream.Session.save(upstream.User(id=1, name="monty"))
    upstream.Session.save(upstream.Project(id=1, name="grail"))
    upstream.Session.save(upstream.Resource(id=1, name="spam"))
    upstream.Session.commit()

def teardown ():
    upstream.metadata.drop_all()
    upstream.metadata.bind = None
    clusterbank.model.upstream.use = None
    clusterbank.model.metadata.bind = None
