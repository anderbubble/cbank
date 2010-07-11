import ConfigParser

import cbank

__all__ = ["test_model", "test_controllers", "test_cli", "test_upstreams"]

_config = None

def setup ():
    for section in cbank.config.sections():
        cbank.config.remove_section(section)
    if cbank.config.has_section("DEFAULT"):
        for option in cbank.config.options("DEFAULT"):
            cbank.config.remove_option("DEFAULT", option)

