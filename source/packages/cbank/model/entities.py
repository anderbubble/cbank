"""model entities

Classes:
User -- user that can use resources
Project -- project to which resources can be allocated
Resource -- resource that can be allocated
Allocation -- record of amount allocated to a project
Hold -- a potential charge against a allocation
Job -- a job run on a resource
Charge -- charge against a allocation
Refund -- refund of a charge
"""


import re
from datetime import datetime, timedelta
import ConfigParser

import decorator


__all__ = [
    "User", "Project", "Resource",
    "Allocation", "Hold", "Charge", "Refund"
]


class Entity (object):
    
    """A generic accounting entity.
    
    Subclasses of this class represent the elements of accounting.
    
    Attributes:
    id -- the entity id
    """
    
    def __init__ (self):
        self.id = None
    
    def __repr__ (self):
        return "<%s id=%r>" % (self.__class__.__name__, self.id)
    
    def __str__ (self):
        if self.id is not None:
            return str(self.id)
        else:
            return "?"

def getattr_ (obj, name, default_thunk):
    try:
        return getattr(obj, name)
    except AttributeError:
        default = default_thunk()
        setattr(obj, name, default)
        return default


@decorator.decorator
def memoized (func, *args):
    dic = getattr_(func, "memoize_dic", dict)
    if args in dic:
        return dic[args]
    else:
        result = func(*args)
        dic[args] = result
        return result


class UpstreamEntity (Entity):

    def __init__ (self, id_):
        Entity.__init__(self)
        self.id = id_

    _in = None
    _out = None

    def __str__ (self):
        str_ = self._out(self.id)
        if str_ is None:
            return str(self.id)
        else:
            return str(str_)

    @classmethod
    def fetch (cls, input):
        id_ = cls._in(input)
        if id_ is None:
            return cls.cached(input)
        else:
            return cls.cached(id_)

    @classmethod
    @memoized
    def cached (cls, *args):
        return cls(*args)

    def __eq__ (self, other):
        return type(self) is type(other) and (str(self.id) == str(other.id))


class User (UpstreamEntity):
    """User that can run jobs and commands."""

    _member = None
    _manager = None

    def is_member (self, project):
        return self._member(project.id, self.id)

    def is_manager (self, project):
        return self._manager(project.id, self.id)


class Project (UpstreamEntity):
    """Project to which resources can be allocated."""


class Resource (UpstreamEntity):
    """Resource that can be allocated to a project."""


class Allocation (Entity):
    
    """An amount of a resource allocated to a project.
    
    Attributes:
    project -- project to which the resource has been allocated
    resource -- resource allocated
    datetime -- when the allocation was entered
    amount -- amount allocated
    start -- when the allocation becomes active
    end -- when the allocation expires
    comment -- misc. comments
    holds -- holds on this allocation
    charges -- charges against this allocation
    
    Properties:
    active -- if the allocation is currently active
    
    Methods:
    amount_charged -- the sum of effective charges
    amount_held -- the sum of active holds
    amount_available -- the amount available to be charged
    """
    
    def __init__ (self, project, resource, amount, start, end):
        """Initialize a new resource allocation.
        
        Arguments:
        project -- the project to allocate to
        resource -- the resource allocated
        amount -- how much of the resource to allocate
        start -- when the allocation becomes active
        end -- when the allocation is no longer active
        """
        Entity.__init__(self)
        self.datetime = datetime.now()
        self.project = project
        self.resource = resource
        self.amount = amount
        self.start = start
        self.end = end
        self.comment = None
        self.holds = []
        self.charges = []

    def _set_resource (self, resource):
        if resource is None:
            self.resource_id = None
        else:
            self.resource_id = resource.id

    def _get_resource (self):
        if self.resource_id is None:
            return None
        else:
            return Resource.cached(self.resource_id)

    resource = property(_get_resource, _set_resource)

    def _set_project (self, project):
        if project is None:
            self.project_id = None
        else:
            self.project_id = project.id

    def _get_project (self):
        if self.project_id is None:
            return None
        else:
            return Project.cached(self.project_id)

    project = property(_get_project, _set_project)
    
    def amount_charged (self):
        """Compute the sum of effective charges (after refunds)."""
        return sum(charge.effective_amount() for charge in self.charges)
    
    def amount_held (self):
        """Compute the sum of the effective amount currently on hold."""
        return sum(hold.amount or 0 for hold in self.holds if hold.active)
    
    def amount_available (self):
        """Compute the amount available for charges."""
        return self.amount - (self.amount_charged() + self.amount_held())
    
    def active (self, now=datetime.now):
        """Determine whether or not this allocation is still active."""
        try:
            now_ = now()
        except TypeError:
            now_ = now
        return self.start <= now_ < self.end


class Hold (Entity):
    
    """Uncharged but unavailable amount of an allocation.
    
    Example:
    A hold may be placed on an account when a job starts, and may be replaced
    with a charge when the job finishes.
    
    Attributes:
    allocation -- allocation to which the hold applies
    job -- job for which the hold exists
    datetime -- when the hold was entered
    amount -- amount held
    comment -- misc. comments
    active -- the hold is active
    
    Classmethods:
    distributed -- construct multiple holds across multiple allocations
    """
    
    def __init__ (self, allocation, amount):
        """Initialize a new hold.
        
        Arguments:
        allocation -- the allocation to hold an amount of
        amount -- how much to hold
        """
        Entity.__init__(self)
        self.datetime = datetime.now()
        self.allocation = allocation
        self.comment = None
        self.active = True
        self.amount = amount
        self.job = None
    
    @classmethod
    def distributed (cls, allocations, amount):
        
        """Construct multiple holds across multiple allocations.
        
        Arguments:
        allocations -- a list of allocations available for holds
        
        Keyword arguments:
        amount -- total amount to be held (required)
        
        Example:
        A project has multiple allocations on a single resource. Use a
        distributed hold to easily hold more funds than any one allocation can
        accomodate.
        """
        
        holds = list()
        for allocation in allocations:
            amount_available = allocation.amount_available()
            if amount_available <= 0:
                continue
            if amount_available >= amount:
                hold = cls(allocation, amount)
            else:
                hold = cls(allocation, amount_available)
            holds.append(hold)
            amount -= hold.amount
            if amount <= 0:
                break
        
        if amount > 0:
            try:
                hold = holds[-1]
            except IndexError:
                try:
                    allocation = allocations[0]
                except IndexError:
                    raise ValueError("no allocation to hold on")
                else:
                    hold = cls(allocation=allocation, amount=amount)
                    holds.append(hold)
                    amount = 0
            else:
                hold.amount += amount
        return holds


class Job (Entity):
    
    """A job run on a computational resource.
    
    Attributes:
    id -- the canonical job id
    user -- the user under which the job executed
    group -- the group under which the job executed
    account -- if the job has an account name string
    name -- the name of the job
    queue -- the name of the queue in which the job executed
    reservation_name -- the name of the resource reservation
    reservation_id -- the id of the resource reservation
    ctime -- when job was created (first submitted)
    qtime -- when job was queued into current queue
    etime -- when job became eligible to run
    start -- when job execution started
    exec_host -- name of host on which the job is being executed
    resource_list -- the specified resource limits
    session -- session number of job
    alternate_id -- optional alternate job identifier
    end -- when job execution ended
    exit_status -- the exit status of the job
    resources_used -- aggregate amount of specified resources used
    accounting_id -- CSA JID, job ID
    charges -- charges associated with the job
    holds -- holds associated with the job
    """
    
    def __init__ (self, id_):
        """Initialize a new job.
        
        Arguments:
        resource -- the resource the job ran on
        id -- the job id
        """
        Entity.__init__(self)
        self.id = id_
        self.user = None
        self.group = None
        self.account = None
        self.name = None
        self.queue = None
        self.reservation_name = None
        self.reservation_id = None
        self.ctime = None
        self.qtime = None
        self.etime = None
        self.start = None
        self.exec_host = None
        self.resource_list = {}
        self.session = None
        self.alternate_id = None
        self.end = None
        self.exit_status = None
        self.resources_used = {}
        self.accounting_id = None
        self.charges = []
        self.holds = []

    def _set_user (self, user):
        if user is None:
            self.user_id = None
        else:
            self.user_id = user.id

    def _get_user (self):
        if self.user_id is None:
            return None
        else:
            return User.cached(self.user_id)

    user = property(_get_user, _set_user)

    def _set_account (self, account):
        if account is None:
            self.account_id = None
        else:
            self.account_id = account.id

    def _get_account (self):
        if self.account_id is None:
            return None
        else:
            return Project.cached(self.account_id)

    account = property(_get_account, _set_account)

    @classmethod
    def from_pbs (cls, entry):
        """Construct a job given a PBS accounting log entry."""
        _, id_, _ = parse_pbs(entry)
        job = cls(id_)
        job.update_from_pbs(entry)
        return job

    def update_from_pbs (self, entry):
        _, _, attributes = parse_pbs(entry)
        if "user" in attributes:
            self.user = User.fetch(attributes['user'])
        if "account" in attributes:
            self.account = Project.fetch(attributes['account'])
        for attribute in ("queue", "group", "exec_host"):
            if attribute in attributes:
                setattr(self, attribute, attributes[attribute])
        for attribute in ("ctime", "qtime", "etime", "start", "end"):
            if attribute in attributes:
                setattr(self, attribute,
                        datetime.fromtimestamp(float(attributes[attribute])))
        if "jobname" in attributes:
            self.name = attributes["jobname"]
        if "Exit_status" in attributes:
            self.exit_status = int(attributes["Exit_status"])
        if "session" in attributes:
            self.session = int(attributes['session'])
        self.resource_list = dict_parser(dict_parser(
                subdict(attributes, "Resource_List."),
            int), parse_timedelta)
        self.resources_used = dict_parser(dict_parser(
                subdict(attributes, "resources_used."),
            int), parse_timedelta)

    def __str__ (self):
        return str(self.id)


class Charge (Entity):
    
    """A charge against an allocation.
    
    Attributes:
    allocation -- allocation to which the charge applies
    amount -- amount charged
    datetime -- when the chage was entered
    comment -- misc. comments
    refunds -- refunds from the charge
    job -- the job associated with this charge
    
    Classmethods:
    distributed -- construct multiple charges across multiple allocations
    
    Methods:
    transfer -- transfer a charged amount to allocations of another project
    amount_refunded -- the sum of refunds of this charge
    effective_amount -- the effective amount of the charge (after refunds)
    """
    
    def __init__ (self, allocation, amount):
        """Initialize a new charge.
        
        Arguments:
        allocation -- the allocation to charge
        amount -- the amount to charge
        """
        Entity.__init__(self)
        self.datetime = datetime.now()
        self.allocation = allocation
        self.comment = None
        self.refunds = []
        self.amount = amount
        self.job = None
    
    @classmethod
    def distributed (cls, allocations, amount):
        
        """Construct multiple charges across multiple allocations.
        
        Arguments:
        allocations -- a list of allocations available for charges
        amount -- total amount to be charged
        
        Example:
        A project has multiple allocations on a single resource. Use a
        distributed charge to charge more funds than any one allocation can
        accomodate.
        """
        
        charges = []
        for allocation in allocations:
            amount_available = allocation.amount_available()
            if amount_available <= 0:
                continue
            if amount_available >= amount:
                charge = cls(allocation, amount)
            else:
                charge = cls(allocation, amount_available)
            amount -= charge.amount
            charges.append(charge)
            if amount <= 0:
                break
        
        if amount > 0:
            try:
                charge = charges[-1]
            except IndexError:
                try:
                    allocation = allocations[0]
                except IndexError:
                    raise ValueError("no allocations to charge against")
                else:
                    charge = cls(allocation, amount)
                    charges.append(charge)
                    amount = 0
            else:
                charge.amount += amount
        
        return charges
    
    def amount_refunded (self):
        """Compute the sum of refunds of the charge."""
        return sum(refund.amount or 0 for refund in self.refunds)
    
    def effective_amount (self):
        """Compute the difference between the charge and refunds."""
        return self.amount - self.amount_refunded()
    
    def transfer (self, project, amount=None):
        """Transfer a charge to allocations of another project.
        
        Returns a tuple containing the refund and a list of new charges.
        
        Arguments:
        project -- the project to transfer the charge to
        amount -- the amount to tranfer (default all)
        """
        if amount is None:
            amount = self.effective_amount()
        charges = project.charge(self.allocation.resource, amount)
        for charge in charges:
            charge.comment = self.comment
            charge.jobs = self.jobs
        refund = self.refund(amount)
        refund.comment = "transferred to %s" % project
        return refund, charges
    
    def refund (self, amount=None):
        """Refund an amount of a charge.
        
        Arguments:
        amount (default all of effective charge)
        """
        return Refund(self, amount)


class Refund (Entity):
    
    """A refund of a charge.
    
    Attributes:
    charge -- charge being refunded
    datetime -- when the charge was entered
    amount -- amount refunded
    comment -- misc. comments
    """
    
    def __init__ (self, charge, amount=None):
        """Initialize a new refund.
        
        Attributes:
        charge -- the charge refunded
        amount -- the amount refunded
        """
        Entity.__init__(self)
        self.datetime = datetime.now()
        self.charge = charge
        if amount is not None:
            self.amount = amount
        else:
            self.amount = charge.effective_amount()
        self.comment = None


def parse_pbs (entry):
    try:
        entry_type, id_, message_text = entry.split(";", 3)[1:]
    except ValueError:
        raise ValueError("Invalid job record: %s" % entry)
    if entry_type not in ("Q", "S", "E"):
        raise ValueError("Invalid job record: %s" % entry)
    attributes = dict(attribute.split("=", 1)
                      for attribute in message_text.split(" ")
                      if "=" in attribute)
    return entry_type, id_, attributes


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
