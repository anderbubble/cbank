from sqlalchemy import create_engine

import elixir

from clusterbank.models import User, Project, Resource
from clusterbank import upstream
from clusterbank.upstream import userbase

__all__ = ["test_models", "test_scripting"]


USERS = [
    dict(
        id = 1,
        name = "user1",
        projects = ["project1", "project2"],
    ),
    dict(
        id = 2,
        name = "user2",
        projects = [],
    )
]

PROJECTS = [
    dict(
        id = 1,
        name = "project1",
    ),
    dict(
        id = 2,
        name = "project2",
    ),
    dict(
        id = 3,
        name = "project3",
    ),
    dict(
        id = 4,
        name = "project4",
    ),
]

RESOURCES = [
    dict(
        id = 1,
        name = "resource1",
    )
]

def setup ():
    elixir.metadata.bind = create_engine("sqlite:///:memory:")
    upstream.User = userbase.User
    upstream.Project = userbase.Project
    upstream.Resource = userbase.Resource
    userbase.model.metadata.bind = create_engine("sqlite:///:memory:")
    userbase.model.metadata.create_all()
    
    for user in USERS:
        userbase.User(id=user['id'], name=user['name'])
    for project in PROJECTS:
        userbase.Project(id=project['id'], name=project['name'])
    for resource in RESOURCES:
        userbase.Resource(id=resource['id'], name=resource['name'])
    userbase.model.context.current.flush()
    
    for user in USERS:
        for project_name in user['projects']:
            userbase.User.by_name(user['name']).projects.append(
                userbase.Project.by_name(project_name))
    userbase.model.context.current.flush()

def teardown ():
    elixir.objectstore.clear()
    elixir.metadata.bind = None
    userbase.model.context.current.clear()
    userbase.model.metadata.bind = None
    del upstream.User
    del upstream.Project
    del upstream.Resource
