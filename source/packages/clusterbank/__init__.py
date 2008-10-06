"""clusterbank

A library for managing and accounting for the use of resources.
"""

import ConfigParser

__all__ = ["config", "__version__"
    "exceptions", "model", "cbank", "upstreams"]

__version__ = "trunk"

config = ConfigParser.SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

