"""cbank

A library for managing and accounting for the use of resources.
"""

import ConfigParser

__version__ = "1.2.0"

config = ConfigParser.SafeConfigParser()
config.read(["/etc/cbank.conf"])

import cbank.model
import cbank.exceptions

from cbank.model import *
from cbank.exceptions import *

__all__ = (cbank.model.__all__ + cbank.exceptions.__all__)
