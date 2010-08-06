from nose.tools import assert_equal

import sqlalchemy.orm

import cbank
import cbank.model


def assert_identical (left, right):
    assert left is right, "%r should be %r" % (left, right)


def assert_not_identical (left, right):
    assert left is not right, "%r should not be %r" % (left, right)


def assert_in (item, container):
    assert item in container, "%s not in %s" % (item, container)


def setup ():
    clear_config()
    cbank.model.clear_upstream()


def clear_config ():
    for section in cbank.config.sections():
        cbank.config.remove_section(section)


def clear_mappers ():
    sqlalchemy.orm.clear_mappers()


def restore_mappers ():
    reload(cbank.model)


class BaseTester (object):

    def setup_engine (self):
        cbank.model.metadata.bind = (
            create_engine("sqlite:///:memory:"))

    def teardown_engine (self):
        cbank.model.metadata.bind = None

    def setup_upstream (self):
        cbank.model.use_upstream(cbank.upstreams.default)

    def teardown_upstream (self):
        cbank.model.clear_upstream()
