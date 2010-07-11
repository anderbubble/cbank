from nose.tools import assert_equal, assert_true

from ConfigParser import ConfigParser

import cbank.cli.common
from cbank.cli.common import get_unit_factor


class ConfigTester (object):

    def setup (self):
        self.saved_config = cbank.config
        cbank.cli.common.config = ConfigParser()
        cbank.cli.common.config.add_section("cli")

    def teardown (self):
        cbank.cli.common.config = self.saved_config


class TestGetUnitFactor (ConfigTester):
    
    def setup (self):
        ConfigTester.setup(self)
        self.warnings = cbank.cli.common.warnings
        cbank.cli.common.warnings = WarningsFake()
    
    def teardown (self):
        ConfigTester.teardown(self)
        cbank.cli.common.warnings = self.warnings
    
    def test_not_configured (self):
        assert_equal(get_unit_factor(), (1, 1))
    
    def test_max (self):
        cbank.cli.common.config.set("cli", "unit_factor", "5/6")
        assert_equal(get_unit_factor(), (5, 6))
    
    def test_no_divisor (self):
        cbank.cli.common.config.set("cli", "unit_factor", "7")
        assert_equal(get_unit_factor(), (7, 1))
    
    def test_unit_factor_invalid (self):
        cbank.cli.common.config.set("cli", "unit_factor", "asdf")
        assert_equal(get_unit_factor(), (1, 1))
        assert_true(cbank.cli.common.warnings.called)


class WarningsFake (object):
    
    def __init__ (self):
        self.called = False
    
    def warn (self, *args, **kwargs):
        self.called = True
