from sqlalchemy import create_engine
from clusterbank.upstream import userbase

__all__ = ["test_model", "test_interface"]

def setup ():
    userbase.model.metadata.bind = create_engine("sqlite:///:memory:")

def teardown ():
    userbase.model.metadata.bind = None
