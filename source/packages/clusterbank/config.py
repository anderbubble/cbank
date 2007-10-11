from ConfigParser import ConfigParser

import sqlalchemy
from sqlalchemy import create_engine
import elixir

from clusterbank import upstream

class ConfigurationError (Exception):
    """An error in the configuration."""
    
    STATUS = 1

class NotConfigured (ConfigurationError):
    """Couldn't load some required configuration."""
    
    STATUS = 2

configured = False

def load_config (config_file="/etc/clusterbank.conf", force=False):
    """Load the configuration from a file into the data structures."""
    global configured
    if configured and not force:
        return
    
    config = ConfigParser()
    try:
        config_file = open(config_file)
    except IOError:
        raise NotConfigured("cannot load configuration: %s" % config_file)
    config.readfp(config_file)
    
    try:
        elixir.metadata.bind = create_engine(config.get("database", "DATABASE_URI"))
    except ImportError, e:
        raise ConfigurationError(e)
    except sqlalchemy.exceptions.ArgumentError, e:
        raise ConfigurationError("local database: %s" % e)
    
    upstream_type = config.get("database", "UPSTREAM_TYPE")
    if upstream_type == "userbase":
        from clusterbank.upstream import userbase
        try:
            userbase.model.metadata.bind = create_engine(config.get("database", "UPSTREAM_DATABASE_URI"))
        except ImportError, e:
            raise ConfigurationError(e)
        except sqlalchemy.exceptions.ArgumentError, e:
            raise ConfigurationError("upstream database: %s" % e)
        upstream.User = userbase.User
        upstream.Project = userbase.Project
        upstream.Resource = userbase.Resource
    else:
        raise NotConfigured("invalid upstream type: %s" % upstream_type)
    configured = True
