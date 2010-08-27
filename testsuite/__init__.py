from nose.tools import assert_equal

import sqlalchemy
import sqlalchemy.orm

import cbank
import cbank.model
import cbank.model.database


def assert_identical (left, right):
    assert left is right, "%r should be %r" % (left, right)


def assert_not_identical (left, right):
    assert left is not right, "%r should not be %r" % (left, right)


def assert_in (item, container):
    assert item in container, "%s not in %s" % (item, container)


def setup ():
    clear_config()
    teardown_upstream()


def teardown_upstream ():
    cbank.model.use_upstream(None)


def clear_config ():
    for section in cbank.config.sections():
        cbank.config.remove_section(section)


def clear_mappers ():
    sqlalchemy.orm.clear_mappers()


def restore_mappers ():
    reload(cbank.model)


class BaseTester (object):

    def setup_database (self):
        cbank.model.database.metadata.bind = (
            sqlalchemy.create_engine("sqlite:///:memory:"))
        cbank.model.database.metadata.create_all()

    def teardown_database (self):
        cbank.model.database.metadata.drop_all()
        cbank.model.database.metadata.bind = None

    def setup_upstream (self):
        cbank.model.use_upstream(cbank.upstreams.default)

    def teardown_upstream (self):
        cbank.model.use_upstream(None)
