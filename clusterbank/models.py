"""Cluster accounting model.

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

This model is based on /ALCF-Admindoc/alcfbank/accounting-db in
the ALCF svn repository.
"""

import warnings
from datetime import datetime

from django.db import models as dm
from django.conf import settings

if settings.UPSTREAM_TYPE == "userbase":
    import clusterbank.upstream.userbase as upstream
else:
    warnings.warn("No upstream layer was loaded.")


class User (dm.Model):
    
    """A logical user.
    
    Relationships:
    request_set -- Requests made by a user.
    allocation_set -- Allocations made by a user.
    credit_limit_set -- CreditLimits posted by the user.
    lien_set -- Liens made by a user.
    charge_set -- Charges made by a user.
    refund_set -- Refunds made by a user.
    unit_factor_set -- Factors set by a user.
    
    Class methods:
    from_upstream_name -- Get a user based on his name upstream.
    
    Attributes:
    upstream_id -- Canonical id of the user.
    can_request -- Permission to make requests.
    can_allocate -- Permission to allocate time or credit.
    can_lien -- Permission to post liens.
    can_charge -- Permission to charge liens.
    can_refund -- Permission to refund charges.
    
    Properties:
    name -- Human-readable name from upstream.
    project_set -- Result set of local projects by upstream membership.
    
    Methods:
    member_of -- Is a user is a member of a given project?
    request -- Request time on a resource for a project.
    allocate -- Allocate time.
    allocate_credit -- Post a credit limit for a project.
    lien -- Acquire a lien against an allocation.
    charge -- Charge a lien.
    refund -- Refund a charge.
    
    Exceptions:
    NotPermitted -- An intentional denial of action.
    NotAMember -- Acting user not a member of the project.
    """
    
    class NotPermitted (Exception):
        """An intentional denail of action."""
    class NotAMember (NotPermitted):
        """Acting user not a member of the project."""
    
    @classmethod
    def from_upstream_name (cls, name):
        """Get (or create) a user based on his upstream name.
        
        Arguments:
        name -- The upstream name of the user.
        """
        try:
            upstream_user = upstream.User.by_name(name)
        except upstream.DoesNotExist:
            raise cls.DoesNotExist("The user does not exist.")
        user, created = cls.objects.get_or_create(upstream_id=upstream_user.id)
        return user
    
    upstream_id = dm.IntegerField(unique=True)
    can_request = dm.BooleanField(default=False)
    can_allocate = dm.BooleanField(default=False)
    can_lien = dm.BooleanField(default=False)
    can_charge = dm.BooleanField(default=False)
    can_refund = dm.BooleanField(default=False)
    
    def _get_name (self):
        """Return the name of the upstream user."""
        upstream_user = upstream.User.by_id(self.upstream_id)
        return upstream_user.name
    name = property(_get_name)
    
    def _get_project_set (self):
        """Return the set of projects that this user is a member of."""
        upstream_projects = upstream.User.by_id(self.upstream_id).projects
        local_projects = (
            Project.objects.get_or_create(upstream_id=project.id)[0]
            for project in upstream_projects)
        local_project_ids = [project.id for project in local_projects]
        return Project.objects.filter(id__in=local_project_ids)
    project_set = property(_get_project_set)
    
    def __str__ (self):
        return self.name
    
    def member_of (self, project):
        """Whether or not a user is a member of a given project."""
        upstream_user = upstream.User.by_id(self.upstream_id)
        upstream_project = upstream.Project.by_id(project.upstream_id)
        return upstream_project in upstream_user.projects
    
    def request (self, **kwargs):
        """Request time on a resource."""
        return Request(poster=self, **kwargs)
    
    def allocate (self, request, **kwargs):
        """Allocate time on a resource in response to a request.
        
        Arguments:
        request -- Which request to allocate time for. (required)
        """
        return request.allocate(poster=self, **kwargs)
    
    def allocate_credit (self, **kwargs):
        """Post a credit limit for a project.
        
        Arguments:
        project -- Which project to give credit to. (required)
        """
        return CreditLimit(poster=self, **kwargs)
    
    def lien (self, allocation=None,
              project=None, resource=None, time=None, **kwargs):
        """Enter a lien against an allocation(s)."""
        if allocation:
            # Allocation is specified.
            # project cannot be specified differently than in allocation.
            if project and project is not allocation.project:
                raise self.NotPermitted(
                    "%s is not an allocation for %s" % (allocation, project))
            # resource cannot be specified differently than in allocation.
            if resource and resource is not allocation.resource:
                raise self.NotPermitted(
                    "%s is not an allocation for %s" % (allocation, resource))
            return Lien(poster=self, allocation=allocation, time=time, **kwargs)
        
        # Allocation is unspecific.
        allocations = Allocation.objects.filter(
            request__project=project, request__resource=resource)
        allocations = allocations.order_by("expiration", "datetime")
        allocations = (
            allocation for allocation in allocations
            if allocation.active
        )
        liens = list()
        for allocation in allocations:
            if allocation.time_available >= time:
                lien = self.lien(allocation, time=time, **kwargs)
            else:
                lien = self.lien(allocation,
                    time = allocation.time_available,
                    **kwargs
                )
            liens.append(lien)
            time -= lien.time
            if time <= 0:
                break
        if time > 0:
            # There is still time to be liened.
            # Add to the last lien.
            try:
                liens[-1].time += time
            except IndexError:
                # No lien has yet been created.
                lien = self.lien(allocation, time=time, **kwargs)
                liens.append(lien)
        return liens
    
    def charge (self, lien=None, liens=None, time=None, **kwargs):
        """Charge time against a lien.
        
        Arguments:
        lien -- lien to charge against. (required)
        """
        if lien:
            # Charge is for a specific lien.
            return lien.charge(poster=self, time=time, **kwargs)
        
        # Apply charge to multiple liens.
        charges = []
        for lien in liens:
            if lien.time_available >= time:
                charge = self.charge(lien, time=time, **kwargs)
            else:
                charge = self.charge(lien, time=lien.time_available, **kwargs)
            charges.append(charge)
            time -= charge.time
            if time <= 0:
                break
        if time > 0:
            # There is still time to be charged.
            # Add to last charge.
            try:
                charges[-1].time += time
            except IndexError:
                # No charges have yet been made.
                charge = self.charge(lien, time=time, **kwargs)
                charges.append(charge)
        return charges
    
    def refund (self, charge, **kwargs):
        """Refund time from a charge.
        
        Arguments:
        charge -- Which charge to refund. (required)
        """
        return charge.refund(poster=self, **kwargs)


class Resource (dm.Model):
    
    """A logical resource.
    
    Relationships:
    request_set -- Requests made for a resource.
    lien_set -- Liens made on a resource.
    
    Class methods:
    from_upstream_name -- Get a resource based on its name upstream.
    
    Attributes:
    upstream_id -- Canonical id of the resource.
    
    Properties:
    name -- Upstream name of the resource.
    """
    
    @classmethod
    def from_upstream_name (cls, name):
        """Get (or create) a resource based on its name upstream.
        
        Arguments:
        name -- The upstream name of the resource.
        """
        try:
            upstream_resource = upstream.Resource.by_name(name)
        except upstream.DoesNotExist:
            raise cls.DoesNotExist("The resource does not exist.")
        resource, created = \
            cls.objects.get_or_create(upstream_id=upstream_resource.id)
        return resource
    
    upstream_id = dm.IntegerField(unique=True)
    
    def _get_name (self):
        """Return the name of the upstream resource."""
        upstream_resource = upstream.Resource.by_id(self.upstream_id)
        return upstream_resource.name
    name = property(_get_name)
    
    def __str__ (self):
        return self.name


class Project (dm.Model):
    
    """A logical project.
    
    Relationships:
    credit_limit_set -- Available credit per-resource.
    request_set -- Requests made by a project.
    lien_set -- Liens made by a project.
    
    Exceptions:
    InsufficientFunds -- Not enough funds to perform an action.
    
    Class methods:
    from_upstream_name -- Get a project based on its name upstream.
    
    Attributes:
    upstream_id -- Canonical id of the project.
    
    Properties:
    name -- The upstream project name.
    user_set -- The users that are members of the project from upstream.
    allocation_set -- All allocations related to this project.
    charge_set -- All charges related to this project.
    lien_set -- All liens related to this project.
    
    Methods:
    has_member -- Is a given user is a member of a project?
    resource_time_allocated -- Sum of time allocated to a resource.
    resource_time_liened -- Sum of time committed to uncharged liens.
    resource_time_charged -- Sum of effective charges.
    resource_time_used -- Sum of time liened and time charged.
    resource_time_available -- Difference of time allocated and time used.
    resource_credit_limit -- Current credit limit for the resource.
    resource_credit_used -- Negative time used.
    resource_credit_available -- Difference of credit limit and credit used.
    """
    
    class InsufficientFunds (Exception):
        """Not enough funds to perform an action."""
    
    @classmethod
    def from_upstream_name (cls, name):
        """Get (or create) a project based on its name upstream.
        
        Arguments:
        name -- The upstream name of the project.
        """
        try:
            upstream_project = upstream.Project.by_name(name)
        except upstream.DoesNotExist:
            raise cls.DoesNotExist("The project does not exist upstream.")
        project, created = \
            cls.objects.get_or_create(upstream_id=upstream_project.id)
        return project
    
    upstream_id = dm.IntegerField(unique=True)
    
    def _get_name (self):
        """Return the name of the upstream project."""
        upstream_project = upstream.Project.by_id(self.upstream_id)
        return upstream_project.name
    name = property(_get_name)
    
    def _get_user_set (self):
        """Return the set of users who are members of this project."""
        upstream_users = upstream.Project.by_id(self.upstream_id).users
        local_users = (User.objects.get_or_create(upstream_id=user.id)[0]
            for user in upstream_users)
        local_user_ids = [user.id for user in local_users]
        return User.objects.filter(id__in=local_user_ids)
    user_set = property(_get_user_set)
    
    def _get_allocation_set (self):
        """Return the set of allocations for this project."""
        return Allocation.objects.filter(request__project=self)
    allocation_set = property(_get_allocation_set)
    
    def _get_lien_set (self):
        """Return the set of liens posted against this project."""
        return Lien.objects.filter(allocation__request__project=self)
    lien_set = property(_get_lien_set)
    
    def _get_charge_set (self):
        """Return the set of charges posted against this project."""
        return Charge.objects.filter(lien__allocation__request__project=self)
    charge_set = property(_get_charge_set)
    
    def __str__ (self):
        return self.name
    
    def has_member (self, user):
        """Whether or not a given user is a member of a project."""
        upstream_project = upstream.Project.by_id(self.upstream_id)
        upstream_user = upstream.User.by_id(user.upstream_id)
        return upstream_user in upstream_project.users
    
    def resource_time_allocated (self, resource):
        """Sum of time in active allocations."""
        allocations = self.allocation_set.filter(request__resource=resource)
        allocations = (
            allocation for allocation in allocations
            if allocation.active
        )
        time_allocated = 0
        for allocation in allocations:
            time_allocated += allocation.time
        return time_allocated
    
    def resource_time_liened (self, resource):
        """Sum of time in active and open liens."""
        liens = self.lien_set.filter(allocation__request__resource=resource)
        liens = (lien for lien in liens if lien.active and lien.open)
        time_liened = 0
        for lien in liens:
            time_liened += lien.time
        return time_liened
    
    def resource_time_charged (self, resource):
        """Sum of time in active charges."""
        charges = self.charge_set.filter(
            lien__allocation__request__resource=resource)
        charges = (charge for charge in charges if charge.active)
        time_charged = 0
        for charge in charges:
            time_charged += charge.effective_charge
        return time_charged
    
    def resource_time_used (self, resource):
        """Sum of time committed to liens and charges."""
        return self.resource_time_liened(resource) \
            + self.resource_time_charged(resource)
    
    def resource_time_available (self, resource):
        """Difference of time allocated and time used."""
        return self.resource_time_allocated(resource) \
            - self.resource_time_used(resource)
    
    def resource_credit_limit (self, resource, datetime=datetime.now):
        """The effective credit limit for a resource at a given date.
        
        Arguments:
        resource -- The applicable resource.
        
        Keyword arguments:
        datetime -- The date to check.
        
        Defaults:
        datetime -- Now.
        """
        try:
            # Allow callable for datetime.
            datetime = datetime()
        except TypeError:
            # Accept standard variable.
            pass
        credit_limits = self.credit_limit_set.filter(
            resource=resource, start__lte=datetime)
        credit_limits = credit_limits.order_by("-start")
        try:
            return credit_limits[0].time
        except IndexError:
            return 0
    
    def resource_credit_used (self, resource):
        delta = self.resource_time_allocated(resource) \
            - self.resource_time_used(resource)
        if delta < 0:
            return -1 * delta
        else:
            return 0
    
    def resource_credit_available (self, resource):
        return self.resource_credit_limit(resource) \
            - self.resource_credit_used(resource)


class CreditLimit (dm.Model):
    
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
    save -- Extends default save behavior.
    
    Constraints:
     * Only one entry for a given project, start, and resource.
    """
    
    class Meta:
        unique_together = (("project", "resource", "start"),)
    
    project = dm.ForeignKey("Project", related_name="credit_limit_set")
    resource = dm.ForeignKey("Resource", related_name="credit_limit_set")
    poster = dm.ForeignKey("User", related_name="credit_limit_set")
    
    start = dm.DateTimeField(default=datetime.now)
    time = dm.IntegerField()
    explanation = dm.TextField(default="")
    
    def __str__ (self):
        return "%s ~%i" % (self.resource.name, self.time)
    
    def save (self):
        """Save a credit limit.
        
        Extends default save method with pre-save checks.
        """
        # Require can_allocate.
        if not self.poster.can_allocate:
            raise self.poster.NotPermitted(
                "%s cannot allocate credit." % self.poster)
        # time cannot be negative
        if self.time is not None and self.time < 0:
            raise ValueError("Credit limit cannot be negative.")
        super(self.__class__, self).save()


class Request (dm.Model):
    
    """A request for time on a resource.
    
    Relationships:
    resource -- The resource to be used.
    project -- The project for which time is requested.
    poster -- The user requesting the time.
    allocation_set -- Allocations on the system in response to this request.
    
    Attributes:
    datetime -- When the request was entered.
    time -- Amount of time requested.
    explanation -- Verbose description of need.
    start -- When the allocation should become active.
    
    Properties:
    active -- The request remains unanswered.
    
    Methods:
    save -- Save a request with pre-save hooks.
    allocate -- Allocate time on a resource in response to a request.
    """
    
    resource = dm.ForeignKey("Resource")
    project = dm.ForeignKey("Project")
    poster = dm.ForeignKey("User")
    
    datetime = dm.DateTimeField(default=datetime.now)
    time = dm.IntegerField()
    explanation = dm.TextField()
    start = dm.DateTimeField(null=True, blank=True)
    
    def __str__ (self):
        try:
            resource_name = self.resource.name
        except Resource.DoesNotExist:
            resource_name = None
        return "%s: %s" % (resource_name, self.time)
    
    def save (self):
        """Save a request.
        
        Extends default save method with pre-save checks.
        """
        # Requestor must have can_request.
        if not self.poster.can_request:
            raise self.poster.NotPermitted(
                "%s cannot make requests." % self.poster)
        # Requestor must be a member of the project.
        if not self.poster.member_of(self.project):
            raise self.poster.NotAMember(
                "%s is not a member of %s." % (self.poster, self.project))
        # Time cannot be negative.
        if self.time is not None and self.time < 0:
            raise ValueError("Cannot request negative time.")
        super(self.__class__, self).save()
    
    def allocate (self, **kwargs):
        """Allocate time on a resource in response to a request."""
        return Allocation(request=self, **kwargs)
    
    def _get_active (self):
        """Whether the request requires consideration."""
        allocated = self.allocation_set.count() > 0
        return not allocated
    active = property(_get_active)


class Allocation (dm.Model):
    
    """An amount of time allocated to a project.
    
    Relationships:
    request -- The request for time to which this is a response.
    poster -- User who entered this allocation into the system.
    charge_set -- Time used from this allocation.
    
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
    
    Methods:
    save -- Save an allocation with pre-save hooks.
    """
    
    request = dm.ForeignKey("Request")
    poster = dm.ForeignKey("User")
    
    approver = dm.CharField(maxlength=30)
    datetime = dm.DateTimeField(default=datetime.now)
    time = dm.IntegerField()
    start = dm.DateTimeField()
    expiration = dm.DateTimeField()
    explanation = dm.TextField(default="")
    
    def __str__ (self):
        return "%s +%i" % (
            self.resource.name,
            self.time)
    
    def _get_project (self):
        """Return the related project."""
        return self.request.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.request.resource
    resource = property(_get_resource)
    
    def save (self):
        """Save an allocation.
        
        Extends default save method with pre-save checks.
        """
        # Allocator must have can_allocate.
        if not self.poster.can_allocate:
            raise self.poster.NotPermitted(
                "%s cannot allocate time." % self.poster)
        # Time cannot be negative.
        if self.time is None:
            self.time = self.request.time
        elif self.time < 0:
            raise ValueError("Cannot allocate negative time.")
        
        # Programmatic defaults.
        if not self.start:
            self.start = self.request.start
        super(self.__class__, self).save()
    
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
    
    def _get_charge_set (self):
        """Return the set of charges made against this allocation."""
        return Charge.objects.filter(lien__allocation=self)
    charge_set = property(_get_charge_set)
    
    def _get_time_charged (self):
        """Return the sum of effective charges against this allocation."""
        time_charged = 0
        for charge in self.charge_set:
            time_charged += charge.effective_charge
        return time_charged
    time_charged = property(_get_time_charged)
    
    def _get_time_liened (self):
        """Sum of time in open liens."""
        time_liened = 0
        for lien in self.lien_set.all():
            if lien.open:
                time_liened += lien.time
        return time_liened
    time_liened = property(_get_time_liened)
    
    def _get_time_available (self):
        return self.time \
            - self.time_liened \
            - self.time_charged
    time_available = property(_get_time_available)


class Lien (dm.Model):
    
    """A potential charge against an allocation.
    
    Relationships:
    allocation -- The allocation this lien is against.
    poster -- The user who posted the lien.
    charge_set -- Charges resulting from this lien.
    
    Exceptions:
    InsufficientFunds -- Charges exceed liens.
    
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
    save -- Save an allocation with pre-save hooks.
    charge -- Charge time against this lien.
    """
    
    allocation = dm.ForeignKey("Allocation")
    poster = dm.ForeignKey("User")
    
    datetime = dm.DateTimeField(default=datetime.now)
    time = dm.IntegerField()
    explanation = dm.TextField(default="")
    
    class InsufficientFunds (Exception):
        """Charges exceed liens."""
    
    def __str__ (self):
        return "%s %i/%i" % (
            self.resource.name,
            self.effective_charge, self.time)
    
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
        for charge in self.charge_set.all():
            effective_charge += charge.effective_charge
        return effective_charge
    effective_charge = property(_get_effective_charge)
    
    def _get_time_available (self):
        """Difference of time liened and effective charge."""
        return self.time - self.effective_charge
    time_available = property(_get_time_available)
    
    def save (self):
        
        """Save a lien.
        
        Extends default save method with pre-save checks.
        """
        
        # Poster must have can_request.
        if not self.poster.can_lien:
            raise self.poster.NotPermitted(
                "%s cannot post liens." % self.poster)
        
        # Poster must be a member of the project.
        if not self.poster.member_of(self.project):
            raise self.poster.NotAMember(
                "%s is not a member of %s." % (self.poster, self.project))
        
        # Time cannot be negative.
        if self.time is not None and self.time < 0:
            raise ValueError("Lien cannot be for negative time.")
        
        super(self.__class__, self).save()
        
        # Cannot take a lien for more than time available + credit.
        credit_limit = self.project.resource_credit_limit(self.resource)
        credit_used = self.project.resource_credit_used(self.resource)
        if credit_used > credit_limit:
            self.delete()
            raise self.project.InsufficientFunds(
                "Credit limit exceeded by %i." % credit_used - credit_limit)
    
    def charge (self, **kwargs):
        """Charge some time against a lien."""
        return Charge(lien=self, **kwargs)
    
    def _get_charged (self):
        """The lien has been charged."""
        return self.charge_set.count() > 0
    charged = property(_get_charged)
    
    def _get_active (self):
        """The lien affects the current allocation."""
        return self.allocation.active
    active = property(_get_active)
    
    def _get_open (self):
        """The lien is still awaiting charges."""
        return not self.charged
    open = property(_get_open)


class Charge (dm.Model):
    
    """A charge against an allocation.
    
    Relationships:
    lien -- The lien to which this charge applies.
    poster -- Who posted the transaction.
    refund_set -- Refunds against this charge.
    
    Exceptions:
    ExcessiveRefund -- Refund in excess of charge.
    
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
    save -- Save a charge with pre-save hooks.
    refund -- Refund time from this charge.
    """
    
    lien = dm.ForeignKey("Lien")
    poster = dm.ForeignKey("User")
    
    datetime = dm.DateTimeField(default=datetime.now)
    time = dm.IntegerField()
    explanation = dm.TextField()
    
    class ExcessiveRefund (Exception):
        """Refund in excess of charge."""
    
    def __str__ (self):
        return "%s -%s" % (
            self.resource.name,
            self.effective_charge)
    
    def _get_effective_charge (self):
        """Difference of charge time and refund times."""
        effective_charge = self.time
        for refund in self.refund_set.all():
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
    
    def save (self):
        
        """Save a lien.
        
        Extends default save method with pre-save checks.
        """
        
        # Require can_charge.
        if not self.poster.can_charge:
            raise self.poster.NotPermitted(
                "%s cannot post charges." % self.poster)
        
        # Time cannot be greater than available time.
        if self.time is not None:
            if self.time > self.lien.time_available:
                raise self.lien.InsufficientFunds(
                    "Total charges cannot exceed lien.")
        
        # No negative charges.
        if self.time is not None and self.time < 0:
            raise ValueError("Cannot charge negative time.")
        
        # Programmatic defaults.
        if self.time is None:
            self.time = self.lien.time
        
        super(self.__class__, self).save()
    
    def refund (self, **kwargs):
        """Refund a portion of the charge."""
        return Refund(charge=self, **kwargs)
    
    def _get_active (self):
        """Charge affects the project's current allocation."""
        return self.lien.active
    active = property(_get_active)


class Refund (dm.Model):
    
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
    save -- Save a refund with pre-save hooks.
    """
    
    charge = dm.ForeignKey("Charge")
    poster = dm.ForeignKey("User")
    
    datetime = dm.DateTimeField(default=datetime.now)
    time = dm.IntegerField()
    explanation = dm.TextField()
    
    def __str__ (self):
        return "%s +%i" % (
            self.resource.name,
            self.time)
    
    def _get_project (self):
        """Return the related project."""
        return self.charge.project
    project = property(_get_project)
    
    def _get_resource (self):
        """Return the related resource."""
        return self.charge.resource
    resource = property(_get_resource)
    
    def save (self):
        
        # Poster must have can_refund.
        if not self.poster.can_refund:
            raise self.poster.NotPermitted("%s cannot refund charges.")
        
        # No negative refunds.
        if self.time is not None and self.time < 0:
            raise ValueError("Cannot refund negative time.")
        
        # Cannot refund more than was charged.
        if self.time is None:
            self.time = self.charge.effective_charge
        elif self.time > self.charge.effective_charge:
            raise self.charge.ExcessiveRefund("Refunds cannot exceed charges.")
        
        super(self.__class__, self).save()
    
    def _get_active (self):
        """The charge affects the project's current allocation."""
        return self.charge.active
    active = property(_get_active)


class UnitFactor (dm.Model):
    
    """A mapping between logical service time and internal resource time.
    
    Relationships:
    poster -- User who added the factor.
    resource -- The resource being described.
    
    Constraints:
     * One entry for a given start and resource.
    
    Class methods:
    resource_factor -- The effective factor for a resource at a given date.
    to_ru -- Convert standard units to resource units.
    to_su -- Convert resource units to standard units.
    
    Attributes:
    start -- When the mapping becomes active.
    factor -- The ratio of su to ru.
    
    su = ru * factor
    
    For example, if one service unit is 1 hour, but a unit of time on
    the resource is 1 minute, the factor would be 60.
    """
    
    class Meta:
        unique_together = (("resource", "start"),)
    
    @classmethod
    def resource_factor (cls, resource, datetime=datetime.now):
        """The effective factor for a resource at a given date.
        
        Arguments:
        resource -- The applicable resource.
        
        Keyword arguments:
        datetime -- The date to check.
        
        Defaults:
        datetime -- Now.
        """
        try:
            datetime = datetime()
        except TypeError:
            pass
        factors = cls.objects.filter(
            resource=resource, start__lte=datetime)
        factors = factors.order_by("-start")
        try:
            return float(factors[0].factor)
        except IndexError:
            return 1.0
    
    @classmethod
    def to_ru (cls, resource, units):
        """Convert standard units to resource units.
        
        Arguments:
        resource -- The resource being used.
        units -- The units to convert.
        """
        return int(units * cls.resource_factor(resource))
    
    @classmethod
    def to_su (cls, resource, units):
        """Convert resource units to standard units.
        
        Arguments:
        resource -- The resource being used.
        units -- The units to convert.
        """
        
        return int(units / cls.resource_factor(resource))
    
    poster = dm.ForeignKey("User")
    resource = dm.ForeignKey("Resource")
    
    start = dm.DateTimeField(default=datetime.now)
    factor = dm.FloatField(max_digits=5, decimal_places=2)
    
    def __str__ (self):
        return "su = %s * %s" % (self.resource, self.factor)
