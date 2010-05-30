from nose.tools import assert_equal, assert_true

from ConfigParser import ConfigParser

import clusterbank.cbank.common
from clusterbank.cbank.common import get_unit_factor


class ConfigTester (object):

    def setup (self):
        self.saved_config = clusterbank.config
        clusterbank.cbank.common.config = ConfigParser()
        clusterbank.cbank.common.config.add_section("cbank")

    def teardown (self):
        clusterbank.cbank.common.config = self.saved_config


class TestGetUnitFactor (ConfigTester):
    
    def setup (self):
        ConfigTester.setup(self)
        self.warnings = clusterbank.cbank.common.warnings
        clusterbank.cbank.common.warnings = WarningsFake()
    
    def teardown (self):
        ConfigTester.teardown(self)
        clusterbank.cbank.common.warnings = self.warnings
    
    def test_not_configured (self):
        assert_equal(get_unit_factor(), (1, 1))
    
    def test_max (self):
        clusterbank.cbank.common.config.set("cbank", "unit_factor", "5/6")
        assert_equal(get_unit_factor(), (5, 6))
    
    def test_no_divisor (self):
        clusterbank.cbank.common.config.set("cbank", "unit_factor", "7")
        assert_equal(get_unit_factor(), (7, 1))
    
    def test_unit_factor_invalid (self):
        clusterbank.cbank.common.config.set("cbank", "unit_factor", "asdf")
        assert_equal(get_unit_factor(), (1, 1))
        assert_true(clusterbank.cbank.common.warnings.called)


class WarningsFake (object):
    
    def __init__ (self):
        self.called = False
    
    def warn (self, *args, **kwargs):
        self.called = True
