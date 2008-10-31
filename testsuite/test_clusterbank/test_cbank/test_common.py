from nose.tools import assert_equal, assert_true

import clusterbank
import clusterbank.cbank.common
from clusterbank.cbank.common import get_unit_factor


class WarningsFake (object):
    
    def __init__ (self):
        self.called = False
    
    def warn (self, *args, **kwargs):
        self.called = True


class TestGetUnitFactor (object):
    
    def setup (self):
        clusterbank.config.add_section("cbank")
        self._warnings = clusterbank.cbank.common.warnings
        clusterbank.cbank.common.warnings = WarningsFake()
    
    def teardown (self):
        clusterbank.config.remove_section("cbank")
        clusterbank.cbank.common.warnings = self._warnings
    
    def test_not_configured (self):
        assert_equal(get_unit_factor(), (1, 1))
    
    def test_max (self):
        clusterbank.config.set("cbank", "unit_factor", "5/6")
        assert_equal(get_unit_factor(), (5, 6))
    
    def test_no_divisor (self):
        clusterbank.config.set("cbank", "unit_factor", "7")
        assert_equal(get_unit_factor(), (7, 1))
    
    def test_unit_factor_invalid (self):
        clusterbank.config.set("cbank", "unit_factor", "asdf")
        assert_equal(get_unit_factor(), (1, 1))
        assert_true(clusterbank.cbank.common.warnings.called)

