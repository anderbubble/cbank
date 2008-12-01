"""Common clusterbank controllers.

Objects:
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
users_property -- property decorator that produces user objects
projects_property -- property decorator that produces project objects
resource_property -- property decorator that produces a resource object
job_from_pbs -- create a job from a pbs entry
"""


from datetime import datetime, timedelta

from clusterbank.model import (upstream, User, Project, Resource,
    Allocation, Hold, Job, Charge, Refund)
from clusterbank.exceptions import InsufficientFunds, NotFound

from sqlalchemy.exceptions import InvalidRequestError
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.session import SessionExtension


__all__ = ["Session", "user", "user_by_id", "user_by_name",
    "project", "project_by_id", "project_by_name",
    "resource", "resource_by_id", "resource_by_name",
    "users_property", "projects_property", "resource_property",
    "job_from_pbs"]


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


Session = scoped_session(sessionmaker(extension=EntityConstraints()))


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


def projects_property (property_):
    """Decorate a property with a project translator.
    
    Given that property_ generates a list of project ids or names,
    generate a list of the equivalent projects.
    """
    def fget (self):
        return [project(project_) for project_ in property_.fget(self)]
    return property(fget)


def users_property (property_):
    """Decorate a property with a user translator.
    
    Given that property_ generates a list of user ids or names,
    generate a list of the equivalent users.
    """
    def fget (self):
        return [user(user_) for user_ in property_.fget(self)]
    return property(fget)


def resource_property (property_):
    """Decorate a property with a resource translator.
    
    Given that property_ generates a resource id or name,
    generate the equivalent resource.
    """
    def fget (self):
        resource_ = property_.fget(self)
        if resource_ is None:
            return None
        else:
            return resource(resource_)
    return property(fget)


def job_from_pbs (entry):
    """Construct a job given a PBS accounting log entry.
    
    Supports Q, S, and E entries.
    """
    id_string, message_text = entry.split(";", 3)[2:]
    job_ = Job(id_string)
    messages = dict(message.split("=", 1)
        for message in message_text.split(" ") if "=" in message)
    job_.queue = messages.get("queue", None)
    try:
        job_.user = user(messages['user'])
    except (KeyError, NotFound):
        pass
    job_.group = messages.get("group", None)
    try:
        job_.account = project(messages['account'])
    except (KeyError, NotFound):
        pass
    job_.name = messages.get("jobname")
    try:
        job_.ctime = datetime.fromtimestamp(float(messages['ctime']))
    except (KeyError, ValueError):
        pass
    try:
        job_.qtime = datetime.fromtimestamp(float(messages['qtime']))
    except (KeyError, ValueError):
        pass
    try:
        job_.etime = datetime.fromtimestamp(float(messages['etime']))
    except (KeyError, ValueError):
        pass
    try:
        job_.start = datetime.fromtimestamp(float(messages['start']))
    except (KeyError, ValueError):
        pass
    try:
        job_.end = datetime.fromtimestamp(float(messages['end']))
    except (KeyError, ValueError):
        pass
    try:
        job_.exit_status = int(messages['Exit_status'])
    except (KeyError, ValueError):
        pass
    job_.exec_host = messages.get("exec_host")
    job_.resource_list = dict_parser(dict_parser(
            subdict(messages, "Resource_List."),
        int), parse_timedelta)
    job_.resources_used = dict_parser(dict_parser(
            subdict(messages, "resources_used."),
        int), parse_timedelta)
    try:
        job_.session = int(messages['session'])
    except (KeyError, ValueError):
        pass
    return job_


def parse_timedelta (timedelta_string):
    """Parse a HH:MM:SS as a timedelta object."""
    try:
        hours, minutes, seconds = timedelta_string.split(":")
    except AttributeError:
        raise ValueError(timedelta_string)
    hours, minutes, seconds = [int(each) for each in (hours, minutes, seconds)]
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def dict_parser (dict_, func):
    """Parse values of a dict using a parsing function.
    
    Arguments:
    dict_ -- the source dictionary
    func -- the function used to parse values
    
    ValueErrors generated by func are silently ignored.
    """
    newdict = {}
    for (key, value) in dict_.iteritems():
        try:
            value = func(value)
        except ValueError:
            pass
        newdict[key] = value
    return newdict


def subdict (dict_, keyroot):
    """Build a subset dict of a dict based on some root key string.
    
    Arguments:
    dict_ -- the primary dict
    keyroot -- the common root string
    
    Example:
    >>> subdict({"key1":1, "key2":2, "otherkey":3}, "key")
    {"1":1, "2":2}
    """
    return dict((key[len(keyroot):], value)
        for (key, value) in dict_.iteritems() if key.startswith(keyroot))

