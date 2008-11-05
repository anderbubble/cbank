"""The clusterbank model.

This package contains the local data model, including reflected projects
and resources from upstream, along with allocations, charges, etc.

Classes:
User -- a user that can charge
Project -- a project that can use a resource
Resource -- a resource that can be allocated
Allocation -- allocation of a resource to a project
Hold -- hold of funds from an account
Job -- job run on a resource
Charge -- charge against an account
Refund -- refund of a charge

Objects:
metadata -- metadata collection
Session -- sessionmaker (and default session)

Functions:
configured_engine -- build a configured SA engine
configured_upstream -- import the configured upstream module
user -- retrieve an upstream user by name or id
project -- retrieve an upstream project by name or id
resource -- retrieve an upstream resource by name or id
user_by_name -- retrieve an upstream user by name
user_by_id -- retrieve an upstream user by id
project_by_name -- retrieve an upstream project by name
project_by_id -- retrieve an upstream project by id
resource_by_name -- retrieve an upstream resource by name
resource_by_id -- retrieve an upstream resource by id
user_projects -- retrieve a users projects
user_projects_owned -- retrieve the projects a user owns
project_members -- retrieve the members of a project
project_owners -- retrieve the owners of a project
job -- create a job from a pbs entry
"""

import warnings
import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.sql import and_
from sqlalchemy.exceptions import InvalidRequestError, ArgumentError
from sqlalchemy.orm import scoped_session, sessionmaker, mapper, relation
from sqlalchemy.orm.session import SessionExtension

from clusterbank import config
from clusterbank.model.entities import upstream, User, Project, \
    Resource, Allocation, Hold, Job, Charge, Refund
from clusterbank.model.database import metadata, \
    users, projects, resources, \
    allocations, holds, jobs, charges, jobs_charges, refunds
from clusterbank.exceptions import InsufficientFunds, NotFound


__all__ = [
    "upstream", "Session",
    "User", "Project", "Resource",
    "Allocation", "Hold", "Job", "Charge", "Refund",
    "user", "user_by_id", "user_by_name",
    "project", "project_by_id", "project_by_name",
    "resource", "resource_by_id", "resource_by_name",
    "user_projects", "user_projects_owned", "project_members",
    "project_owners",
]


def configured_engine ():
    """Build a configured SQLAlchemy engine."""
    try:
        uri = config.get("main", "database")
    except ConfigParser.Error:
        warnings.warn("no database specified", UserWarning)
        engine = None
    else:
        try:
            engine = create_engine(uri)
        except (ImportError, ArgumentError), ex:
            warnings.warn(
                "invalid database: %s (%s)" % (uri, ex), UserWarning)
            engine = None
    return engine


def configured_upstream ():
    """Import the configured upstream module."""
    try:
        module_name = config.get("upstream", "module")
    except ConfigParser.Error:
        module_name = "clusterbank.upstreams.default"
    try:
        module = __import__(module_name, locals(), globals(), [
            "get_user_name", "get_user_id",
            "get_project_name", "get_project_id",
            "get_resource_name", "get_resource_id",
            "get_project_members", "get_project_owners",
            "get_member_projects", "get_owner_projects"])
    except ImportError:
        warnings.warn(
            "invalid upstream module: %s" % (module_name), UserWarning)
        module = None
    return module


class EntityConstraints (SessionExtension):
    
    """SQLAlchemy SessionExtension containing entity constraints.
    
    Methods (constraints):
    check_amounts -- require entity amounts to be positive
    check_holds -- require new holds to fit in their allocations
    check_refunds -- require new refunds to fit in their charges
    """
    
    def check_amounts (self, session):
        """Require new entities to have positive amounts."""
        for entity_ in (session.new | session.dirty):
            if isinstance(entity_, Allocation):
                if entity_.amount < 0:
                    raise ValueError(
                        "invalid amount for allocation: %r" % entity_.amount)
            elif isinstance(entity_, Hold):
                if entity_.amount < 0:
                    raise ValueError(
                        "invalid amount for hold: %r" % entity_.amount)
            elif isinstance(entity_, Charge):
                if entity_.amount < 0:
                    raise ValueError(
                        "invalid amount for charge: %r" % entity_.amount)
            elif isinstance(entity_, Refund):
                if entity_.amount < 0:
                    raise ValueError(
                        "invalid amount for refund: %r" % entity_.amount)
    
    def check_holds (self, session):
        """Require new holds to fit in their allocations."""
        holds_ = (instance for instance in (session.new | session.dirty)
            if isinstance(instance, Hold))
        for allocation in set(hold.allocation for hold in holds_):
            if allocation.amount_available() < 0:
                raise InsufficientFunds("not enough funds available")
    
    def check_refunds (self, session):
        """Require new refunds to fit in their allocations."""
        refunds_ = (instance for instance in (session.new | session.dirty)
            if isinstance(instance, Refund))
        charges_ = set(refund.charge for refund in refunds_)
        for charge in charges_:
            if charge.effective_amount() < 0:
                raise InsufficientFunds("not enough funds available")
    
    def before_commit (self, session):
        """Check constraints before committing."""
        self.check_amounts(session)
        self.check_holds(session)
        self.check_refunds(session)


mapper(User, users, properties={
    'id':users.c.id})


mapper(Project, projects, properties={
    'id':projects.c.id})


mapper(Resource, resources, properties={
    'id':resources.c.id})


mapper(Allocation, allocations, properties={
    'id':allocations.c.id,
    'project':relation(Project, backref="allocations"),
    'resource':relation(Resource, backref="allocations"),
    'datetime':allocations.c.datetime,
    'amount':allocations.c.amount,
    'start':allocations.c.start,
    'expiration':allocations.c.expiration,
    'comment':allocations.c.comment})


mapper(Hold, holds, properties={
    'id':holds.c.id,
    'allocation':relation(Allocation, backref="holds"),
    'datetime':holds.c.datetime,
    'user':relation(User, backref="holds"),
    'amount':holds.c.amount,
    'comment':holds.c.comment,
    'active':holds.c.active})


mapper(Job, jobs, properties={
    'id':jobs.c.id,
    'resource':relation(Resource, backref="jobs"),
    'charges':relation(Charge, backref="jobs", secondary=jobs_charges)})


mapper(Charge, charges, properties={
    'id':charges.c.id,
    'allocation':relation(Allocation, backref="charges"),
    'datetime':charges.c.datetime,
    'user':relation(User, backref="charges"),
    'amount':charges.c.amount,
    'comment':charges.c.comment})


mapper(Refund, refunds, properties={
    'id':refunds.c.id,
    'charge':relation(Charge, backref="refunds"),
    'datetime':refunds.c.datetime,
    'amount':refunds.c.amount,
    'comment':refunds.c.comment})


def user (name_or_id):
    """Construct a user from its name or id."""
    return entity(User, name_or_id,
        upstream.get_user_id, upstream.get_user_name)


def user_by_id (id_):
    """Construct a user from its id."""
    return entity_by_id(User, id_, upstream.get_user_name)


def user_by_name (name):
    """Construct a user from its name."""
    return entity_by_name(User, name, upstream.get_user_id)


def project (name_or_id):
    """Construct a project from its name or id."""
    return entity(Project, name_or_id,
        upstream.get_project_id, upstream.get_project_name)


def project_by_id (id_):
    """Construct a project from its id."""
    return entity_by_id(Project, id_, upstream.get_project_name)


def project_by_name (name):
    """Construct a project from its name."""
    return entity_by_name(Project, name, upstream.get_project_id)


def resource (name_or_id):
    """Construct a resource from its name or id."""
    return entity(Resource, name_or_id,
        upstream.get_resource_id, upstream.get_resource_name)


def resource_by_id (id_):
    """Construct a resource from its id."""
    return entity_by_name(Resource, id_, upstream.get_resource_name)


def resource_by_name (name):
    """Construct a resource from its name."""
    return entity_by_name(Resource, name, upstream.get_resource_id)


def entity (cls, name_or_id, get_id, get_name):
    """Construct an entity of type cls from its name or id.
    
    Arguments:
    cls -- the UpstreamEntity subclass to construct
    name_or_id -- the name or id to use to look up the entity
    get_id -- upstream function to retrieve the entity id from its name
    get_name -- upstream function to retrieve the entity name from its id
    """
    try:
        return entity_by_name(cls, name_or_id, get_id)
    except NotFound:
        return entity_by_id(cls, name_or_id, get_name)


def entity_by_name (cls, name, get_id):
    """Construct an entity of type cls from its name.
    
    Arguments:
    cls -- the UpstreamEntity subclass to construct
    name -- the name to use to look up the entity
    get_id -- upstream function to retrieve the entity id from its name
    """
    id_ = get_id(name)
    if id_ is None:
        raise NotFound("%s %r not found" % (cls.__name__.lower(), name))
    return _entity_by_id(cls, id_)


def entity_by_id (cls, id_, get_name):
    """Construct an entity of type cls from its id.
    
    Arguments:
    cls -- the UpstreamEntity subclass to construct
    id_ -- the id to use to look up the entity
    get_name -- upstream function to retrieve the entity name from its id
    """
    if get_name(id_) is None:
        raise NotFound("%s %r not found" % (cls.__name__.lower(), id_))
    return _entity_by_id(cls, id_)


def _entity_by_id (cls, id_):
    """Construct an entity of type cls from its id.
    
    Arguments:
    cls -- the UpstreamEntity subclass to construct
    id_ -- the id to use to look up the entity
    get_name -- upstream function to retrieve the entity name from its id
    
    Note:
    This function does not check with upstream to guarantee that an entity
    with the id exists. For most cases, use entity_by_id in stead.
    """
    s = Session()
    try:
        return s.query(cls).filter_by(id=id_).one()
    except InvalidRequestError:
        entity_ = cls(id_)
        s.add(entity_)
        return entity_


def user_projects (user_):
    """Get the projects that the given user is a member of."""
    return [project_by_id(project_id) for project_id in user_.projects]


def user_projects_owned (user_):
    """Get the projects that the given user owns."""
    return [project_by_id(project_id) for project_id in user_.projects_owned]


def project_members (project_):
    """Get the users the are a member of the given project."""
    return [user_by_id(user_id) for user_id in project_.members]


def project_owners (project_):
    """Get the users that own the given project."""
    return [user_by_id(user_id) for user_id in project_.owners]

def job (entry):
    record_type, id_string, message_text = entry.split(";", 3)[1:]
    job_ = Job(id_string)
    messages = dict(message.split("=", 1)
        for message in message_text.split(" "))
    job_.queue = messages['queue']
    return job_


Session = scoped_session(sessionmaker(extension=EntityConstraints()))
metadata.bind = configured_engine()
upstream.use = configured_upstream()

