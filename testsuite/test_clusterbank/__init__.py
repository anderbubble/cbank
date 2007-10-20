from sqlalchemy import create_engine

import clusterbank.model
from clusterbank.model import User, Project, Resource
from clusterbank import upstream
from clusterbank.upstream import userbase

__all__ = ["test_models", "test_scripting"]

def setup ():
    clusterbank.model.metadata.bind = create_engine("sqlite:///:memory:")
    clusterbank.model.metadata.create_all()
    upstream.User = userbase.User
    upstream.Project = userbase.Project
    upstream.Resource = userbase.Resource
    userbase.model.metadata.bind = create_engine("sqlite:///:memory:")
    userbase.model.metadata.create_all()
    
    monty = userbase.User(id=1, name="Monty")
    userbase.User(id=2, name="Python")
    grail = userbase.Project(id=1, name="grail")
    meaning = userbase.Project(id=2, name="meaning")
    userbase.Project(id=3, name="circus")
    userbase.Project(id=4, name="brian")
    monty.projects = [grail, meaning]
    userbase.Resource(id=1, name="spam")
    userbase.Session.flush()

def teardown ():
    clusterbank.model.Session.clear()
    clusterbank.model.metadata.drop_all()
    clusterbank.model.metadata.bind = None
    userbase.Session.clear()
    userbase.model.metadata.drop_all()
    userbase.model.metadata.bind = None
    del upstream.User
    del upstream.Project
    del upstream.Resource
