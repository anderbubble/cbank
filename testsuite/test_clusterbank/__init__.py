import ConfigParser

import clusterbank

__all__ = ["test_model", "test_controllers", "test_cbank", "test_upstreams"]

_config = None

def setup ():
    for section in clusterbank.config.sections():
        clusterbank.config.remove_section(section)
    if clusterbank.config.has_section("DEFAULT"):
        for option in clusterbank.config.options("DEFAULT"):
            clusterbank.config.remove_option("DEFAULT", option)

