"""Cluster accounting model.

Before using the models:

 elixir.metadata.connect(<uri>) # connect model to a database
 UpstreamEntity = <UpstreamClass> # provide upstream class to local classes
 # other configuration may be required for upstream
 

Classes:
User -- A user in the system.
Resource -- A resource (usually for computation).
Project -- A project in the system.
Request -- Request for time on a resource.
Allocation -- Record of time allocated to a project.
CreditLimit -- A maximum negative value for a project on a resource.
Lien -- A potential charge against a allocation.
Charge -- Charge against a allocation.
Refund -- Refund against a charge.
UnitFactor -- Translation of model time to physical time.
"""

from datetime import datetime

from sqlalchemy import Integer, DateTime, Unicode, Boolean, desc, UniqueConstraint
import elixir
from elixir import Entity, Field, has_many, with_fields, belongs_to
import clusterbank.statements


__all__ = [
    "User", "Project", "Resource",
    "fetch_user", "fetch_project", "fetch_resource",
    "Request", "Allocation", "CreditLimit", "Lien", "Charge", "Refund",
]

def fetch_user (name):
    """Get (or create) a user based on its name upstream.
    
    Arguments:
    name -- The upstream name of the user.
    """
    try:
        upstream_user = User.UpstreamEntity.by_name(name)
    except User.UpstreamEntity.DoesNotExist:
        raise User.DoesNotExist("The user does not exist.")
    user = User.get_by(id=upstream_user.id) \
        or User(id=upstream_user.id)
    elixir.objectstore.flush([user])
    return user


class User (Entity):
    
    """A logical user.
    
    Relationships:
    credit_limits -- CreditLimits posted by the user.
    requests -- Requests made by the user.
    allocations -- Allocations made by the user.
    liens -- Liens made by the user.
    charges -- Charges made by the user.
    refunds -- Refunds made by the user.
    unit_factors -- Factors set by the user.
    
    Attributes:
    id -- Canonical id of the user.
    can_request -- Permission to make requests.
    can_allocate -- Permission to allocate time or credit.
    can_lien -- Permission to post liens.
    can_charge -- Permission to charge liens.
    can_refund -- Permission to refund charges.
    
    Properties:
    name -- Human-readable name from upstream.
    projects -- Result set of local projects by upstream membership.
    
    Methods:
    member_of -- A user is a member of a project.
    request -- Request time on a resource for a project.
    allocate -- Allocate time for a request.
    allocate_credit -- Allocate a credit limit for a project.
    lien -- Acquire a lien against an allocation.
    charge -- Charge a lien.
    refund -- Refund a charge.
    
    Exceptions:
    DoesNotExist -- The specified user does not exist.
    NotPermitted -- An intentional denial of an action.
    """
    
    has_many("credit_limits", of_kind="CreditLimit")
    has_many("requests", of_kind="Request")
    has_many("allocations", of_kind="Allocation")
    has_many("liens", of_kind="Lien")
    has_many("charges", of_kind="Charge")
    has_many("refunds", of_kind="Refund")
    has_many("unit_factors", of_kind="UnitFactor")
    
    with_fields(
        id = Field(Integer, primary_key=True),
        can_request = Field(Boolean, required=True, default=False),
        can_allocate = Field(Boolean, required=True, default=False),
        can_lien = Field(Boolean, required=True, default=False),
        can_charge = Field(Boolean, required=True, default=False),
        can_refund = Field(Boolean, required=True, default=False),
    )
    
    class DoesNotExist (Exception):
        """The specified user does not exist."""
    
    class NotPermitted (Exception):
        """An intentional denail of an action."""
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def __str__ (self):
        return self.name
    
    def _get_name (self):
        """Return the name of the upstream user."""
        upstream_user = self.UpstreamEntity.by_id(self.id)
        return upstream_user.name
    name = property(_get_name)
    
    def _get_projects (self):
        """Return the set of projects that this user is a member of."""
        upstream_projects = self.UpstreamEntity.by_id(self.id).projects
        local_projects = [
            fetch_project(name)
            for name in (project.name for project in upstream_projects)
        ]
        return local_projects
    projects = property(_get_projects)
    
    def member_of (self, project):
        """Whether or not a user is a member of a given project."""
        upstream_user = self.UpstreamEntity.by_id(self.id)
        upstream_project = Project.UpstreamEntity.by_id(project.id)
        return upstream_project in upstream_user.projects
    
    def request (self, **kwargs):
        """Request time on a resource."""
        return Request(poster=self, **kwargs)
    
    def allocate (self, **kwargs):
        """Allocate time on a resource in response to a request.
        
        Arguments:
        request -- Which request to allocate time for. (required)
        """
        return Allocation(poster=self, **kwargs)
    
    def allocate_credit (self, **kwargs):
        """Post a credit limit for a project.
        
        Arguments:
        project -- Which project to give credit to. (required)
        """
        return CreditLimit(poster=self, **kwargs)
    
    def lien (self, **kwargs):
        
        """Enter a lien against an allocation."""
        
        if kwargs.get("allocation") is not None:
            kwargs.pop('project', None)
            kwargs.pop('resource', None)
            return Lien(poster=self, **kwargs)
        
        kwargs.pop("allocation", None)
        
        project = kwargs.pop('project')
        resource = kwargs.pop('resource')
        time = kwargs.pop('time')
        
        allocations = Allocation.query().join("request").filter_by(project=project, resource=resource)
        allocations = allocations.order_by([Allocation.c.expiration, Allocation.c.datetime])
        allocations = (
            allocation for allocation in allocations
            if allocation.active
        )
        
        # Post a lien to each allocation until all time has been liened.
        liens = list()
        for allocation in allocations:
            # If the remaining time will fit into the allocation,
            # post a lien for it. Otherwise, post a lien for whatever
            # The allocation will support.
            if allocation.time_available >= time:
                lien = self.lien(allocation=allocation, time=time, **kwargs)
            else:
                lien = self.lien(allocation=allocation, time=allocation.time_available, **kwargs)
            liens.append(lien)
            time -= lien.time
            if time <= 0:
                break
        
        # If there is still time to be liened, add it to the last lien.
        # This allows liens to be posted that put the project negative.
        if time > 0:
            try:
                liens[-1].time += time
            except IndexError:
                # No lien has yet been created. Post a lien on the last
                # Allocation used.
                try:
                    lien = Lien(allocation=allocation, time=time, **kwargs)
                except NameError:
                    # There are no allocations.
                    raise self.NotPermitted("There are no active allocations for %s on %s." % (project, resource))
                liens.append(lien)
        return liens
    
    def charge (self, **kwargs):
        
        """Charge time against a lien.
        
        Arguments:
        lien -- lien to charge against.
        liens -- liens to which the charge can be charged.
        """
        
        # If the charge is for a specific lien, post the charge.
        if kwargs.get("lien") is not None:
            kwargs.pop("liens", None)
            return Charge(poster=self, **kwargs)
        
        kwargs.pop("lien", None)
        
        # No specific lien has been given. Post a charge to each lien
        # in the pool until all time has been charged.
        charges = list()
        time = kwargs.pop("time")
        for lien in kwargs.pop("liens"):
            # If the remaining time will fit into the lien, post a
            # Charge for all of it. Otherwise, post a charge for what
            # the lien can support.
            if lien.time_available >= time:
                charge = self.charge(lien=lien, time=time, **kwargs)
            else:
                charge = self.charge(lien=lien, time=lien.time_available, **kwargs)
            charges.append(charge)
            time -= charge.time
            # Iterate through all liens to charge 0 on unused liens.
            # Charging 0 marks the lien as closed, and frees the liened
            # time.
            #if time <= 0:
            #    break
        
        # If there is time remaining, add it to the last charge.
        if time > 0:
            try:
                charges[-1].time += time
            except IndexError:
                # No charges have yet been made. Charge the last lien.
                try:
                    charge = self.charge(lien=lien, time=time, **kwargs)
                except NameError:
                    # There was no lien.
                    raise self.NotPermitted("No liens are available to be charged.")
                charges.append(charge)
        return charges
    
    def refund (self, **kwargs):
        """Refund time from a charge.
        
        Arguments:
        charge -- Which charge to refund. (required)
        """
        return Refund(poster=self, **kwargs)


def fetch_project (name):
    """Get (or create) a project based on its upstream name.
    
    Arguments:
    name -- The upstream name of the project.
    """
    try:
        upstream_project = Project.UpstreamEntity.by_name(name)
    except Project.UpstreamEntity.DoesNotExist:
        raise Project.DoesNotExist("The project does not exist.")
    project = Project.get_by(id=upstream_project.id) \
        or Project(id=upstream_project.id)
    elixir.objectstore.flush([project])
    return project

class Project (Entity):
    
    """A logical project.
    
    Relationships:
    credit_limits -- Available credit per-resource.
    requests -- Requests made for the project.
    
    Exceptions:
    DoesNotExist -- The specified project does not exist.
    InsufficientFunds -- Not enough funds to perform an action.
    
    Attributes:
    id -- Canonical id of the project.
    
    Properties:
    name -- The upstream project name.
    users -- The users that are members of the project from upstream.
    allocations -- All allocations related to this project.
    charges -- All charges related to this project.
    liens -- All liens related to this project.
    
    Methods:
    has_member -- The group has a member.
    time_allocated -- Sum of time allocated to a resource.
    time_liened -- Sum of time committed to uncharged liens.
    time_charged -- Sum of effective charges.
    time_used -- Sum of time liened and time charged.
    time_available -- Difference of time allocated and time used.
    credit_limit -- Current credit limit for the resource.
    credit_used -- Negative time used.
    credit_available -- Difference of credit limit and credit used.
    """
    
    has_many("credit_limits", of_kind="CreditLimit")
    has_many("requests", of_kind="Request")
    
    with_fields(
        id = Field(Integer, primary_key=True),
    )
    
    class DoesNotExist (Exception):
        """The specified project does not exist."""
    
    class InsufficientFunds (Exception):
        """Not enough funds to perform an action."""
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def __str__ (self):
        return self.name
    
    def _get_name (self):
        """Return the name of the upstream project."""
        upstream_project = self.UpstreamEntity.by_id(self.id)
        return upstream_project.name
    name = property(_get_name)
    
    def _get_users (self):
        """Return the set of users who are members of this project."""
        upstream_users = self.UpstreamEntity.by_id(self.id).users
        local_users = [
            fetch_user(name)
            for name in (user.name for user in upstream_users)
        ]
        return local_users
    users = property(_get_users)
    
    def has_member (self, user):
        """Whether or not a given user is a member of a project."""
        upstream_project = self.UpstreamEntity.by_id(self.id)
        upstream_user = User.UpstreamEntity.by_id(user.id)
        return upstream_user in upstream_project.users
    
    def _get_allocations (self):
        """Return the set of allocations for this project."""
        return Allocation.query().join("request").filter_by(project=self)
    allocations = property(_get_allocations)
    
    def _get_liens (self):
        """Return the set of liens posted against this project."""
        return Lien.query().join("request").filter_by(project=self)
    liens = property(_get_liens)
    
    def _get_charges (self):
        """Return the set of charges posted against this project."""
        return Charge.query().join("request").filter_by(project=self)
    charges = property(_get_charges)
    
    def time_allocated (self, resource):
        """Sum of time in active allocations."""
        allocations = Allocation.query().join("request").filter_by(
            project = self,
            resource = resource,
        )
        allocations = (
            allocation for allocation in allocations
            if allocation.active
        )
        time_allocated = 0
        for allocation in allocations:
            time_allocated += allocation.time
        return time_allocated
    
    def time_liened (self, resource):
        """Sum of time in active and open liens."""
        liens = Lien.query().join(["allocation", "request"]).filter_by(
            project = self,
            resource = resource,
        )
        liens = (
            lien for lien in liens
            if lien.active and lien.open
        )
        time_liened = 0
        for lien in liens:
            time_liened += lien.time
        return time_liened
    
    def time_charged (self, resource):
        """Sum of time in active charges."""
        charges = Charge.query().join(["lien", "allocation", "request"]).filter_by(
            project = self,
            resource = resource,
        )
        charges = (
            charge for charge in charges
            if charge.active
        )
        time_charged = 0
        for charge in charges:
            time_charged += charge.effective_charge
        return time_charged
    
    def time_used (self, resource):
        """Sum of time committed to liens and charges."""
        return self.time_liened(resource) \
            + self.time_charged(resource)
    
    def time_available (self, resource):
        """Difference of time allocated and time used."""
        return self.time_allocated(resource) \
            - self.time_used(resource)
    
    def credit_limit (self, resource):
        """The effective credit limit for a resource at a given date.
        
        Arguments:
        resource -- The applicable resource.
        """
        credit_limits = CreditLimit.query().filter(
            CreditLimit.c.start <= datetime.now(),
        ).filter_by(
            project = self,
            resource = resource,
        ).order_by(desc(CreditLimit.c.start))
        try:
            return credit_limits[0].time
        except IndexError:
            return 0
    
    def credit_used (self, resource):
        delta = self.time_allocated(resource) \
            - self.time_used(resource)
        if delta < 0:
            return -1 * delta
        else:
            return 0
    
    def credit_available (self, resource):
        return self.credit_limit(resource) \
            - self.credit_used(resource)


def fetch_resource (name):
    """Get (or create) a resource based on its name upstream.
    
    Arguments:
    name -- The upstream name of the resource.
    """
    try:
        upstream_resource = Resource.UpstreamEntity.by_name(name)
    except Resource.UpstreamEntity.DoesNotExist:
        raise Resource.DoesNotExist("The resource does not exist.")
    resource = Resource.get_by(id=upstream_resource.id) \
        or Resource(id=upstream_resource.id)
    elixir.objectstore.flush([resource])
    return resource


class Resource (Entity):
    
    """A logical resource.
    
    Relationships:
    credit_limits -- CreditLimits on the resource.
    requests -- Requests made for the resource.
    unit_factors -- UnitFactors for this resource.
    
    Attributes:
    id -- Canonical id of the resource.
    
    Properties:
    name -- Upstream name of the resource.
    
    Exceptions:
    DoesNotExist -- The specified resource does not exist.
    """
    
    has_many("credit_limits", of_kind="CreditLimit")
    has_many("requests", of_kind="Request")
    
    with_fields(
        id = Field(Integer, primary_key=True),
    )
    
    class DoesNotExist (Exception):
        """The specified resource does not exist."""
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def __str__ (self):
        return self.name
    
    def _get_name (self):
        """Return the name of the upstream resource."""
        upstream_resource = Resource.UpstreamEntity.by_id(self.id)
        return upstream_resource.name
    name = property(_get_name)


class CreditLimit (Entity):
    
    """A limit on the charges a project can have.
    
    Relationships:
    project -- Related project.
    resource -- Applicable resource.
    poster -- Who set this credit limit.
    
    Attributes:
    start -- When this credit limit becomes active.
    time -- Amount of credit available.
    explanation -- A verbose explanation of why credit was allocated.
    
    Methods:
    
    Constraints:
     * Only one entry for a given project, start, and resource.
    """
    
    belongs_to("project", of_kind="Project", required=True)
    belongs_to("resource", of_kind="Resource", required=True)
    belongs_to("poster", of_kind="User", required=True)
    
    with_fields(
        id = Field(Integer, primary_key=True),
        start = Field(DateTime, required=True, default=datetime.now),
        time = Field(Integer, required=True),
        explanation = Field(Unicode),
    )
    
    #using_table_options(
    #    UniqueConstraint("project_id", "resource_id", "start"),
    #)
    
    clusterbank.statements.before_insert("_check_permissions", "_check_values")
    clusterbank.statements.before_update("_check_permissions", "_check_values")
    
    def __str__ (self):
        return "%s ~%i" % (self.resource.name, self.time)
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _check_permissions (self):
        """Check that the poster has permission to allocate credit."""
        if not self.poster.can_allocate:
            raise self.poster.NotPermitted(
                "%s cannot allocate credit." % self.poster)
    
    def _check_values (self):
        """Check that the allocation values are valid."""
        if self.time < 0 and self.time is not None:
            raise ValueError("Credit limit cannot be negative.")

# Move this to the class definition when the Elixir dependency bug is fixed.
CreditLimit._descriptor.add_constraint(
        UniqueConstraint("project_id", "resource_id", "start"),
)

class Request (Entity):
    
    """A request for time on a resource.
    
    Relationships:
    resource -- The resource to be used.
    project -- The project for which time is requested.
    poster -- The user requesting the time.
    allocations -- Allocations on the system in response to this request.
    
    Attributes:
    datetime -- When the request was entered.
    time -- Amount of time requested.
    explanation -- Verbose description of need.
    start -- When the allocation should become active.
    
    Properties:
    open -- The request remains unanswered.
    
    Methods:
    allocate -- Allocate time on a resource in response to a request.
    """
    
    belongs_to("resource", of_kind="Resource", required=True)
    belongs_to("project", of_kind="Project", required=True)
    belongs_to("poster", of_kind="User", required=True)
    
    has_many("allocations", of_kind="Allocation")
    
    with_fields(
        id = Field(Integer, primary_key=True),
        datetime = Field(DateTime, required=True, default=datetime.now),
        time = Field(Integer, required=True),
        explanation = Field(Unicode),
        start = Field(DateTime),
    )
    
    clusterbank.statements.before_insert("_check_permissions", "_check_values")
    clusterbank.statements.before_update("_check_permissions", "_check_values")
    
    def __str__ (self):
        try:
            resource_name = self.resource.name
        except Resource.DoesNotExist:
            resource_name = None
        return "%s ?%s" % (resource_name, self.time)
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _check_permissions (self):
        """Check that the poster has permission to request."""
        if not self.poster.can_request:
            raise self.poster.NotPermitted(
                "%s cannot make requests." % self.poster)
        if not (self.poster.member_of(self.project) or self.poster.can_allocate):
            raise self.poster.NotPermitted(
                "%s is not a member of %s." % (self.poster, self.project))
    
    def _check_values (self):
        """Check that the values of the request are valid."""
        if self.time is not None and self.time < 0:
            raise ValueError("Cannot request negative time.")
    
    def _get_allocated (self):
        """Whether the request has had time allocated to it."""
        return len(self.allocations) > 0
    allocated = property(_get_allocated)
    
    def _get_open (self):
        """Whether the request is awaiting a reply."""
        return not self.allocated
    open = property(_get_open)


class Allocation (Entity):
    
    """An amount of time allocated to a project.
    
    Relationships:
    request -- The request for time to which this is a response.
    poster -- User who entered the allocation into the system.
    charges -- Time used from the allocation.
    
    Attributes:
    datetime -- When the allocation was entered.
    approver -- The person/group who approved the allocation.
    time -- Amount of time allocated.
    start -- When the allocation becomes active.
    explanation -- Verbose description of the allocation.
    
    Properties:
    project -- Project from associated request.
    resource -- Resource from associated request.
    started -- Allocation has started.
    expired -- Allocation has expired.
    active -- The allocation has started and has not expired.
    """
    
    belongs_to("request", of_kind="Request")
    belongs_to("poster", of_kind="User")
    
    has_many("liens", of_kind="Lien")
    
    with_fields(
        id = Field(Integer, primary_key=True),
        approver = Field(Unicode),
        datetime = Field(DateTime, required=True, default=datetime.now),
        time = Field(Integer, required=True),
        start = Field(DateTime, required=True),
        expiration = Field(DateTime, required=True),
        explanation = Field(Unicode),
    )
    
    clusterbank.statements.before_insert("_check_permissions", "_check_values", "_set_programmatic_defaults")
    clusterbank.statements.before_update("_check_permissions", "_check_values")
    
    def __str__ (self):
        return "%s +%i" % (self.resource.name, self.time)
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _get_project (self):
        """Return the related project."""
        return self.request.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.request.resource
    resource = property(_get_resource)
    
    def _check_permissions (self):
        """Check that the poster has permission to allocate."""
        if not self.poster.can_allocate:
            raise self.poster.NotPermitted(
                "%s cannot allocate time." % self.poster)
    
    def _check_values (self):
        """Check that the values of the allocation are valid."""
        if self.time is None:
            self.time = self.request.time
        elif self.time < 0:
            raise ValueError("Cannot allocate negative time.")
        
    def _set_programmatic_defaults (self):
        if not self.start:
            self.start = self.request.start
    
    def _get_started (self):
        """The allocation has a start date before now."""
        return self.start <= datetime.now()
    started = property(_get_started)
    
    def _get_expired (self):
        """The allocation has an expiration date before now."""
        return self.expiration <= datetime.now()
    expired = property(_get_expired)
    
    def _get_active (self):
        """The allocation's time affect's the project's time."""
        return self.started and not self.expired
    active = property(_get_active)
    
    def _get_charges (self):
        """Return the set of charges made against this allocation."""
        return Charge.query().filter_by(allocation=self)
    charges = property(_get_charges)
    
    def _get_time_charged (self):
        """Return the sum of effective charges against this allocation."""
        time_charged = 0
        for charge in self.charges:
            time_charged += charge.effective_charge
        return time_charged
    time_charged = property(_get_time_charged)
    
    def _get_time_liened (self):
        """Sum of time in open liens."""
        time_liened = 0
        for lien in self.liens:
            if lien.open:
                time_liened += lien.time
        return time_liened
    time_liened = property(_get_time_liened)
    
    def _get_time_available (self):
        return self.time \
            - self.time_liened \
            - self.time_charged
    time_available = property(_get_time_available)


class Lien (Entity):
    
    """A potential charge against an allocation.
    
    Relationships:
    allocation -- The allocation the lien is against.
    poster -- The user who posted the lien.
    charges -- Charges resulting from the lien.
    
    Attributes:
    datetime -- When the lien was entered.
    time -- How many time could be charged.
    explanation -- Verbose description of the lien.
    
    Properties:
    project -- Points to related project.
    resource -- Points to related resource.
    effective_charge -- Total time charged (after refunds).
    time_available -- Difference of time and effective_charge.
    charged -- The lien has charges.
    active -- The lien is against an active allocation.
    open -- The lien is uncharged.
    
    Methods:
    charge -- Charge time against this lien.
    
    Exceptions:
    InsufficientFunds -- Charges exceed liens.
    """
    
    belongs_to("allocation", of_kind="Allocation", required=True)
    belongs_to("poster", of_kind="User", required=True)
    
    has_many("charges", of_kind="Charge")
    
    with_fields(
        id = Field(Integer, primary_key=True),
        datetime = Field(DateTime, required=True, default=datetime.now),
        time = Field(Integer, required=True),
        explanation = Field(Unicode),
    )
    
    clusterbank.statements.before_insert("_check_permissions", "_check_values_pre")
    clusterbank.statements.after_insert("_check_values_post")
    clusterbank.statements.before_update("_check_permissions", "_check_values_pre")
    clusterbank.statements.after_update("_check_values_post")
    
    class InsufficientFunds (Exception):
        """Charges exceed liens."""
    
    def __str__ (self):
        return "%s %i/%i" % (
            self.resource.name,
            self.effective_charge, self.time
        )
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _get_project (self):
        """Return the related project."""
        return self.allocation.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.allocation.resource
    resource = property(_get_resource)
    
    def _get_effective_charge (self):
        """Sum the effective charges of related charges for this lien."""
        effective_charge = 0
        for charge in self.charges:
            effective_charge += charge.effective_charge
        return effective_charge
    effective_charge = property(_get_effective_charge)
    
    def _get_time_available (self):
        """Difference of time liened and effective charge."""
        return self.time - self.effective_charge
    time_available = property(_get_time_available)
    
    def _check_permissions (self):
        """Check that the poster has permission to lien."""
        if not self.poster.can_lien:
            raise self.poster.NotPermitted(
                "%s cannot post liens." % self.poster)
        if not (self.poster.member_of(self.project) or self.poster.can_charge):
            raise self.poster.NotPermitted(
                "%s is not a member of %s." % (self.poster, self.project))
    
    def _check_values_pre (self):
        """Check that the value of the lien is valid."""
        if self.time < 0 and self.time is not None:
            raise ValueError("Lien cannot be for negative time.")
        
    def _check_values_post (self):
        """Check that the value of the lien is valid."""
        credit_limit = self.project.credit_limit(self.resource)
        credit_used = self.project.credit_used(self.resource)
        if credit_used > credit_limit:
            self.delete()
            raise self.project.InsufficientFunds(
                "Credit limit exceeded by %i." % (credit_used - credit_limit))
    
    def _get_charged (self):
        """The lien has been charged."""
        return len(self.charges) > 0
    charged = property(_get_charged)
    
    def _get_active (self):
        """The lien affects the current allocation."""
        return self.allocation.active
    active = property(_get_active)
    
    def _get_open (self):
        """The lien is still awaiting charges."""
        return not self.charged
    open = property(_get_open)


class Charge (Entity):
    
    """A charge against an allocation.
    
    Relationships:
    lien -- The lien to which this charge applies.
    poster -- Who posted the transaction.
    refunds -- Refunds against this charge.
    
    Attributes:
    datetime -- When the charge was deducted.
    time -- Amount of time used.
    explanation -- A verbose description of the charge.
    
    Properties:
    effective_charge -- The unit charge after any refunds.
    project -- project from related request.
    resource -- resource from related request.
    active -- The charge is against an active lien.
    
    Methods:
    refund -- Refund time from this charge.
    
    Exceptions:
    ExcessiveRefund -- Refund in excess of charge.
    """
    
    belongs_to("lien", of_kind="Lien", required=True)
    belongs_to("poster", of_kind="User", required=True)
    
    has_many("refunds", of_kind="Refund")
    
    with_fields(
        id = Field(Integer, primary_key=True),
        datetime = Field(DateTime, required=True, default=datetime.now),
        time = Field(Integer, required=True),
        explanation = Field(Unicode),
    )
    
    clusterbank.statements.before_insert("_check_permissions", "_check_values", "_set_programmatic_defaults")
    clusterbank.statements.before_update("_check_permissions", "_check_values")
    
    class ExcessiveRefund (Exception):
        """Refund in excess of charge."""
    
    def __str__ (self):
        return "%s -%s" % (
            self.resource.name,
            self.effective_charge
        )
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _get_effective_charge (self):
        """Difference of charge time and refund times."""
        effective_charge = self.time
        for refund in self.refunds:
            effective_charge -= refund.time
        return effective_charge
    effective_charge = property(_get_effective_charge)
    
    def _get_project (self):
        """Return the related project."""
        return self.lien.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.lien.resource
    resource = property(_get_resource)
    
    def _check_permissions (self):
        """Check that the poster has permission to charge."""
        if not self.poster.can_charge:
            raise self.poster.NotPermitted(
                "%s cannot post charges." % self.poster)
    
    def _check_values (self):
        """Check that the values of the charge are valid."""
        if self.time is not None and self.time < 0:
            raise ValueError("Cannot charge negative time.")
    
    def _set_programmatic_defaults (self):
        if self.time is None:
            self.time = self.lien.time
    
    def _get_active (self):
        """Charge affects the project's current allocation."""
        return self.lien.active
    active = property(_get_active)


class Refund (Entity):
    
    """A refund against a charge.
    
    Relationships:
    charge -- The charge being refunded.
    poster -- Who posted the refund.
    
    Attributes:
    datetime -- When the refund was added.
    time -- How much time was refunded.
    explanation -- A (possibly verbose) description of the refund.
    
    Properties:
    project -- Project from associated charge.
    resource -- Resource from associated charge.
    active -- The refund is against an active charge.
    
    Methods:
    """
    
    belongs_to("charge", of_kind="Charge", required=True)
    belongs_to("poster", of_kind="User", required=True)
    
    with_fields(
        id = Field(Integer, primary_key=True),
        datetime = Field(DateTime, required=True, default=datetime.now),
        time = Field(Integer, required=True),
        explanation = Field(Unicode),
    )
    
    clusterbank.statements.before_insert("_check_permissions", "_check_values", "_set_programmatic_defaults")
    clusterbank.statements.before_update("_check_permissions", "_check_values")
    
    def __str__ (self):
        return "%s +%i" % (self.resource.name, self.time)
    
    def __repr__ (self):
        if self.id is None:
            id_repr = "?"
        else:
            id_repr = self.id
        return "<%s %s>" % (self.__class__.__name__, id_repr)
    
    def _get_project (self):
        """Return the related project."""
        return self.charge.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.charge.resource
    resource = property(_get_resource)
    
    def _check_permissions (self):
        """Check that the poster has permission to refund."""
        if not self.poster.can_refund:
            raise self.poster.NotPermitted(
                "%s cannot refund charges." % self.poster)
    
    def _check_values (self):
        """Check that the value of the refund is valid."""
        if self.time < 0 and self.time is not None:
            raise ValueError("Cannot refund negative time.")
        elif self.time > self.charge.effective_charge:
            raise self.charge.ExcessiveRefund("Refunds cannot exceed charges.")
        
    def _set_programmatic_defaults (self):
        if self.time is None:
            self.time = self.charge.effective_charge
    
    def _get_active (self):
        """The charge affects the project's current allocation."""
        return self.charge.active
    active = property(_get_active)


# class UnitFactor (Entity):
#     
#     """A mapping between logical service time and internal resource time.
#     
#     Relationships:
#     poster -- User who added the factor.
#     resource -- The resource being described.
#     
#     Constraints:
#      * One entry for a given start and resource.
#     
#     Class methods:
#     resource_factor -- The effective factor for a resource at a given date.
#     to_ru -- Convert standard units to resource units.
#     to_su -- Convert resource units to standard units.
#     
#     Attributes:
#     start -- When the mapping becomes active.
#     factor -- The ratio of su to ru.
#     
#     su = ru * factor
#     
#     For example, if one service unit is 1 hour, but a unit of time on
#     the resource is 1 minute, the factor would be 60.
#     """
#     
#     belongs_to("poster", of_kind="User", required=True)
#     belongs_to("resource", of_kind="Resource", required=True)
#     
#     with_fields(
#         id = Field(Integer, primary_key=True),
#         start = Field(DateTime, required=True, default=datetime.now),
#         factor = Field(Integer, required=True),
#         explanation = Field(Unicode),
#     )
#     
#     #using_table_options(
#     #    UniqueConstraint("resource_id", "start"),
#     #)
#     
#     @classmethod
#     def resource_factor (cls, resource):
#         """The effective factor for a resource at a given date.
#         
#         Arguments:
#         resource -- The applicable resource.
#         """
#         factors = cls.query().filter_by(
#             resource = resource,
#         ).filter(
#             UnitFactor.c.start <= datetime.now(),
#         ).order_by(desc(UnitFactor.c.start))
#         try:
#             return float(factors[0].factor)
#         except IndexError:
#             return 1.0
#     
#     @classmethod
#     def to_ru (cls, resource, units):
#         """Convert standard units to resource units.
#         
#         Arguments:
#         resource -- The resource being used.
#         units -- The units to convert.
#         """
#         return int(units * cls.resource_factor(resource))
#     
#     @classmethod
#     def to_su (cls, resource, units):
#         """Convert resource units to standard units.
#         
#         Arguments:
#         resource -- The resource being used.
#         units -- The units to convert.
#         """
#         
#         return int(units / cls.resource_factor(resource))
#     
#     def __str__ (self):
#         return "su = %s * %s" % (self.resource, self.factor)
# 
# # Move this to the class definition when the Elixir dependency bug is fixed.
# UnitFactor._descriptor.add_constraint(
#     UniqueConstraint("resource_id", "start"),
# )
