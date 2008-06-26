import ConfigParser

import clusterbank.model

__all__ = ["test_model", "test_cbank", "test_upstreams"]

clusterbank.model.entities.config = ConfigParser.SafeConfigParser()
