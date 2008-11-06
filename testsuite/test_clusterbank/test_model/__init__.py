from sqlalchemy import create_engine

import clusterbank.model.database
from clusterbank.upstreams import default as upstream
from clusterbank.upstreams.default import User, Project, Resource

__all__ = ["test_entities", "test_accounting"]

def setup ():
    clusterbank.model.database.metadata.bind = \
        create_engine("sqlite:///:memory:")
    clusterbank.model.upstream.use = upstream
    upstream.users = [User(1, "monty")]
    upstream.projects = [Project(1, "grail")]
    upstream.resources = [Resource(1, "spam")]

def teardown ():
    upstream.users = []
    upstream.projects = []
    upstream.resources = []
    clusterbank.model.upstream.use = None
    clusterbank.model.metadata.bind = None

