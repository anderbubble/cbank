"""cbank

A library for managing and accounting for the use of resources.
"""

import ConfigParser

__all__ = ["config", "__version__", "exceptions", "model", "controllers",
    "cli", "upstreams"]

__version__ = "trunk"

config = ConfigParser.SafeConfigParser()
config.read(["/etc/cbank.conf"])

# import common elements for convenience
from cbank.model import *
from cbank.controllers import *
from cbank.exceptions import *

# update __all__
import cbank.model
import cbank.controllers
import cbank.exceptions
__all__ = (__all__ + cbank.model.__all__ +
    cbank.controllers.__all__ + cbank.exceptions.__all__)
