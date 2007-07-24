import os
import ConfigParser

config = ConfigParser.ConfigParser()
config.read(["/etc/clusterbank.conf"])

DATABASE_URI = config.get("database", "DATABASE_URI")
UPSTREAM_DATABASE_URI = config.get("database", "UPSTREAM_DATABASE_URI")
UPSTREAM_TYPE = config.get("database", "UPSTREAM_TYPE")
