import warnings
from ConfigParser import SafeConfigParser, NoSectionError, NoOptionError

from sqlalchemy import create_engine

import clusterbank.model
import clusterbank.upstream

__all__ = ["model", "scripting", "upstream"]

__version__ = "0.2.x"

config = SafeConfigParser()
config.read(["/etc/clusterbank.conf"])

try:
    uri = config.get("main", "database")
except (NoSectionError, NoOptionError):
    warnings.warn("no database specified", ImportWarning)
else:
    try:
        clusterbank.model.metadata.bind = create_engine(uri)
    except Exception, e:
        warnings.warn("invalid database: %s (%s)" % (uri, e), ImportWarning)
