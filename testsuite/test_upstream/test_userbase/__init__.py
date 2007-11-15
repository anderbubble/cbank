from sqlalchemy import create_engine
from clusterbank.upstream import userbase

__all__ = ["test_interface"]

def setup ():
    userbase.metadata.bind = create_engine("sqlite:///:memory:")

def teardown ():
    userbase.metadata.bind = None
