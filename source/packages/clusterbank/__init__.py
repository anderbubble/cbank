"""clusterbank

A library for managing and accounting for the use of resources.

Packages:
model -- data model
"""

import ConfigParser

__all__ = ["config", "model", "interfaces", "__version__"]

__version__ = "trunk"

config = ConfigParser.SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

