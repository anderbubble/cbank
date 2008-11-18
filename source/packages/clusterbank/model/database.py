"""clusterbank data model metadata.

The metadata provides sqlalchemy with information as to how the database
is layed out, so that it can map local classes to a relational database.

Objects:
metadata -- master metadata object
projects -- projects
resources -- resources
allocations -- allocations
holds -- holds
jobs -- jobs run on a resource
charges -- charges
refunds -- refunds
"""


from datetime import datetime, timedelta

from sqlalchemy import MetaData, Table, Column, ForeignKey
from sqlalchemy.types import TypeDecorator, Integer, DateTime, \
    Text, Boolean, String


__all__ = [
    "metadata",
    "users", "projects", "resources",
    "allocations", "holds", "jobs", "charges", "jobs_charges", "refunds",
]


class Dictionary (TypeDecorator):
    
    """Store a dict as text.
    
    Automatically converts timedeltas and ints.
    """
    
    impl = Text
    
    def process_bind_param (self, value, engine):
        """Convert a dict to a space-separated key=value string."""
        if value is None:
            return None
        pairs = []
        for (key, value_) in value.iteritems():
            if isinstance(value_, timedelta):
                value_str = "%i:%i:%i" % (
                    value_.days, value_.seconds, value_.microseconds)
            else:
                value_str = str(value_)
            pairs.append("%s=%s" % (key, value_str))
        return " ".join(pairs)
    
    def process_result_value (self, value, engine):
        """Convert a space-separated key=value string to a dict.
        
        Automatically converts timedeltas and ints in value.
        """
        if value is None:
            return None
        valuedict = {}
        for pair in value.split(" "):
            try:
                key, value = pair.split("=", 1)
            except ValueError:
                continue
            else:
                valuedict[key] = value
        parsers = [int, float,
            lambda val:timedelta(*[int(v)
                for v in valuedict[key].split(":", 2)])]
        for key in valuedict:
            for parser in parsers:
                try:
                    valuedict[key] = parser(valuedict[key])
                except ValueError:
                    continue
                else:
                    break
        return valuedict


metadata = MetaData()


users = Table("users", metadata,
    Column("id", Integer, primary_key=True),
    mysql_engine="InnoDB")


projects = Table("projects", metadata,
    Column("id", Integer, primary_key=True),
    mysql_engine="InnoDB")


resources = Table("resources", metadata,
    Column("id", Integer, primary_key=True),
    mysql_engine="InnoDB")


allocations = Table("allocations", metadata,
    Column("id", Integer, primary_key=True),
    Column("project_id", None, ForeignKey("projects.id"), nullable=False),
    Column("resource_id", None, ForeignKey("resources.id"), nullable=False),
    Column("datetime", DateTime, nullable=False, default=datetime.now),
    Column("amount", Integer, nullable=False),
    Column("start", DateTime, nullable=False),
    Column("expiration", DateTime, nullable=False),
    Column("comment", Text),
    mysql_engine="InnoDB")


holds = Table("holds", metadata,
    Column("id", Integer, primary_key=True),
    Column("allocation_id", None, ForeignKey("allocations.id"), nullable=False),
    Column("datetime", DateTime, nullable=False, default=datetime.now),
    Column("amount", Integer, nullable=False),
    Column("user_id", None, ForeignKey("users.id"), nullable=True),
    Column("comment", Text),
    Column("active", Boolean, nullable=False, default=True),
    mysql_engine="InnoDB")


jobs = Table("jobs", metadata,
    Column("id", String(255), primary_key=True, autoincrement=False),
    Column("user_id", None, ForeignKey("users.id"), nullable=True),
    Column("group", String(255), nullable=True),
    Column("account_id", None, ForeignKey("projects.id"), nullable=True),
    Column("name", String(255), nullable=True),
    Column("queue", String(255), nullable=True),
    Column("reservation_name", String(255), nullable=True),
    Column("reservation_id", String(255), nullable=True),
    Column("ctime", DateTime, nullable=True),
    Column("qtime", DateTime, nullable=True),
    Column("etime", DateTime, nullable=True),
    Column("start", DateTime, nullable=True),
    Column("exec_host", String(255), nullable=True),
    Column("resource_list", Dictionary, nullable=True),
    Column("session", Integer, nullable=True),
    Column("alternate_id", String(255), nullable=True),
    Column("end", DateTime, nullable=True),
    Column("exit_status", Integer, nullable=True),
    Column("resources_used", Dictionary, nullable=True),
    Column("accounting_id", String(255), nullable=True),
    mysql_engine="InnoDB")


charges = Table("charges", metadata,
    Column("id", Integer, primary_key=True),
    Column("allocation_id", None, ForeignKey("allocations.id"), nullable=False),
    Column("datetime", DateTime, nullable=False, default=datetime.now),
    Column("amount", Integer, nullable=False),
    Column("user_id", None, ForeignKey("users.id"), nullable=True),
    Column("comment", Text),
    mysql_engine="InnoDB")


jobs_charges = Table("jobs_charges", metadata,
    Column("job_id", None, ForeignKey("jobs.id")),
    Column("charge_id", None, ForeignKey("charges.id")),
    mysql_engine="InnoDB")


refunds = Table("refunds", metadata,
    Column("id", Integer, primary_key=True),
    Column("charge_id", None, ForeignKey("charges.id"), nullable=False),
    Column("datetime", DateTime, nullable=False, default=datetime.now),
    Column("amount", Integer, nullable=False),
    Column("comment", Text),
    mysql_engine="InnoDB")

