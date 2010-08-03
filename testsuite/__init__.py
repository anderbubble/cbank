from nose.tools import assert_equal

import cbank.model


def assert_identical (left, right):
    assert left is right, "%r should be %r" % (left, right)


def assert_not_identical (left, right):
    assert left is not right, "%r should not be %r" % (left, right)


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
