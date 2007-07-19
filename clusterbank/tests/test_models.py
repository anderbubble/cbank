from datetime import datetime, timedelta

import django.core.management
import django.db.models.query
try:
    import settings # Assumed to be in the same directory.
except ImportError:
    print >> sys.stderr, "Error: Can't find the file 'settings.py' in the directory containing %r. It appears you've customized things." % __file__
    print >> sys.stderr, "You'll have to run django-admin.py, passing it your settings module."
    print >> sys.stedrr, "(If the file settings.py does indeed exist, it's causing an ImportError somehow.)"
    sys.exit(1)
else:
    django.core.management.setup_environ(settings)
from clusterbank import models

def tag (*names):
    def tagger (func):
        try:
            func.tags.extend(names)
        except AttributeError:
            func.tags = list(names)
        return func
    return tagger


UPSTREAM_TEST_USER = dict(
    id = 258,
    name = "abate",
    member_of = ["grp-petrol"],
    not_member_of = ["foam"],
)

UPSTREAM_INVALID_USER = dict(
    id = 0,
    name = "",
)

UPSTREAM_TEST_PROJECT = dict(
    id = 1,
    name = "grp-petrol",
    has_member = ['abate'],
    not_has_member = ['janderso'],
)

UPSTREAM_INVALID_PROJECT = dict(
    id = 0,
    name = "",
)

UPSTREAM_TEST_RESOURCE = dict(
    id = 18,
    name = "lcrc",
)

UPSTREAM_INVALID_RESOURCE = dict(
    id = 0,
    name = "",
)

REQUEST_AMOUNT = 2000
ALLOCATION_AMOUNT = 1200
CREDIT_LIMIT_AMOUNT = 100
LIEN_AMOUNT = 900
CHARGE_AMOUNT = 300
REFUND_AMOUNT = 100
STANDARD_UNITS = 100
RESOURCE_UNITS = 30
UNIT_FACTOR = 0.3


def setup ():
    """Initialize the local database before any tests are run.
    
    Note that upstream is assumed to already exist.
    """
    django.core.management.syncdb(verbosity=0, interactive=False)


class TestModel (object):
    
    def teardown (self):
        """Reset the database after each test."""
        django.core.management.reset(models, interactive=False)


class TestUser (TestModel):
    """Tester for the user model."""
    
    # Class methods
    @tag("user")
    def test_from_upstream_name (self):
        """Fetch an upstream user into the system by name."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        assert isinstance(user, models.User)
    
    @tag("user")
    def test_from_upstream_invalid_name (self):
        """Fetching an invalid user raises an exception."""
        try:
            user = models.User.from_upstream_name(UPSTREAM_INVALID_USER['name'])
        except models.User.DoesNotExist:
            pass
        else:
            assert False
    
    # Attributes
    @tag("user")
    def test_upstream_id (self):
        """user.upstream_id reflects the id of the upstream user."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        assert user.upstream_id == UPSTREAM_TEST_USER['id']
    
    @tag("user")
    def test_defaults (self):
        """User permissions start False."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        assert not user.can_request
        assert not user.can_allocate
        assert not user.can_lien
        assert not user.can_charge
        assert not user.can_refund
    
    # Properties
    @tag("user")
    def test_name (self):
        """user.name reflects the upstream name."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        assert user.name == UPSTREAM_TEST_USER['name']
    
    @tag("user")
    def test_project_set (self):
        """user.project_set reflects upstream project membership."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        assert isinstance(user.project_set, django.db.models.query.QuerySet)
        if user.project_set.count() < 1:
            raise Exception("Unable to verify instance classes. (No instances.)")
        for project in user.project_set:
            assert isinstance(project, models.Project)
    
    # Methods
    @tag("user")
    def test_member_of (self):
        """Valid membership.
        
        Local users are members of local groups if the corresponding
        upstream user is a member of corresponding upstream groups.
        """
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        member_projects = \
            [models.Project.from_upstream_name(upstream_name)
             for upstream_name in UPSTREAM_TEST_USER['member_of']]
        not_member_projects = \
            [models.Project.from_upstream_name(upstream_name)
             for upstream_name in UPSTREAM_TEST_USER['not_member_of']]
        for project in member_projects:
            assert user.member_of(project)
        for project in not_member_projects:
            assert not user.member_of(project)
    
    @tag("user")
    def test_request (self):
        """A user can request time on a project."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        request = user.request()
        assert isinstance(request, models.Request)
    
    @tag("user")
    def test_allocate (self):
        """A user can allocate time for a request."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        request = user.request()
        allocation = user.allocate(request)
        assert isinstance(allocation, models.Allocation)
    
    @tag("user")
    def test_allocate_credit (self):
        """A user can allocate credit."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        credit = user.allocate_credit()
        assert isinstance(credit, models.CreditLimit)
    
    @tag("user")
    def test_lien (self):
        """A user can take out a lien against an allocation."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        request = user.request()
        allocation = user.allocate(request)
        lien = user.lien(allocation)
        assert isinstance(lien, models.Lien)
    
    @tag("user")
    def test_lien_nonspecific (self):
        """A user can take out a lien against a project and resource.
        
        The user distributes the lien across any available allocations for
        the project/resource pair.
        """
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        # Request two allocations for less than will be liened.
        request = user.request(
            project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = user.allocate(request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = LIEN_AMOUNT / 2,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        request = user.request(
            project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = user.allocate(request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = LIEN_AMOUNT / 2,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        # Post a lien that must be split across both.
        liens = user.lien(
            project = request.project,
            resource = request.resource,
            time = LIEN_AMOUNT,
        )
        assert len(liens) == 2
        assert liens[0].time == LIEN_AMOUNT / 2
        assert liens[1].time == LIEN_AMOUNT / 2
    
    @tag("user")
    def test_charge (self):
        """A user can charge against a lien."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        request = user.request()
        allocation = user.allocate(request)
        lien = user.lien(allocation)
        charge = user.charge(lien)
        assert isinstance(charge, models.Charge)
    
    @tag("user")
    def test_charge_nonspecific (self):
        """A user can charge against multiple liens."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        # Request two allocations for less than will be liened.
        request = user.request(
            project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = user.allocate(request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = LIEN_AMOUNT / 2,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        request = user.request(
            project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = user.allocate(request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = LIEN_AMOUNT / 2,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        # Post a lien that must be split across both.
        liens = user.lien(
            project = request.project,
            resource = request.resource,
            time = LIEN_AMOUNT,
        )
        for lien in liens:
            lien.poster.can_lien = True
            lien.save()
        # Post charges across both liens.
        charges = user.charge(liens=liens, time=(LIEN_AMOUNT / 2) + (LIEN_AMOUNT / 4))
        assert len(charges) == 2
        assert charges[0].time == LIEN_AMOUNT / 2
        assert charges[1].time == LIEN_AMOUNT / 4
    
    @tag("user")
    def test_refund (self):
        """A user can refund a charge."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        request = user.request()
        allocation = user.allocate(request)
        lien = user.lien(allocation)
        charge = user.charge(lien)
        refund = user.refund(charge)
        assert isinstance(refund, models.Refund)


class TestResource (TestModel):
    
    # Class methods
    @tag("resource")
    def test_from_upstream_name (self):
        """Fetch an upstream resource into the system by name."""
        resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name'])
        assert isinstance(resource, models.Resource)
    
    @tag("resource")
    def test_from_upstream_invalid_name (self):
        """Fetching an invalid upstream resource raises an exception."""
        try:
            resource = models.Resource.from_upstream_name(UPSTREAM_INVALID_RESOURCE['name'])
        except models.Resource.DoesNotExist:
            pass
        else:
            assert False
    
    # Attributes
    @tag("resource")
    def test_upstream_id (self):
        """resource.upstream_id reflects the id of the upstream resource."""
        resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name'])
        assert resource.upstream_id == UPSTREAM_TEST_RESOURCE['id']
    
    # Properties
    @tag("resource")
    def test_name (self):
        """resource.name reflect the upstream name of the resource."""
        resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name'])
        assert resource.name == UPSTREAM_TEST_RESOURCE['name']


class TestProject (TestModel):
    
    # Class methods
    @tag("project")
    def test_from_upstream_name (self):
        """Fetch an upstream project into the local system by name."""
        project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name'])
        assert isinstance(project, models.Project)
    
    @tag("project")
    def test_from_upstream_invalid_name (self):
        """Fetching an invalid upstream project raises an exception."""
        try:
            project = models.Project.from_upstream_name(UPSTREAM_INVALID_PROJECT['name'])
        except models.Project.DoesNotExist:
            pass
        else:
            assert False
    
    # Attributes
    @tag("project")
    def test_upstream_id (self):
        """project.upstream_id reflects the id of the upstream project."""
        project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name'])
        assert project.upstream_id == UPSTREAM_TEST_PROJECT['id']
    
    # Properties
    @tag("project")
    def test_name (self):
        """project.name reflects the upstream name."""
        project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name'])
        assert project.name == UPSTREAM_TEST_PROJECT['name']
    
    @tag("project")
    def test_user_set (self):
        """Local projects have local user members based on upstream membership."""
        project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name'])
        assert isinstance(project.user_set, django.db.models.query.QuerySet)
        if project.user_set.count() < 1:
            raise Exception("Unable to verify instance classes. (No instances.)")
        for user in project.user_set:
            assert isinstance(user, models.User)
    
    # Methods
    @tag("project")
    def test_has_member (self):
        """Valid membership.
        
        Local groups have local users as members if the corresponding
        upstream group has, as a member, corresponding upstream users.
        """
        project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name'])
        member_users = \
            [models.User.from_upstream_name(upstream_name)
             for upstream_name in UPSTREAM_TEST_PROJECT['has_member']]
        not_member_users = \
            [models.User.from_upstream_name(upstream_name)
             for upstream_name in UPSTREAM_TEST_PROJECT['not_has_member']]
        for user in member_users:
            assert project.has_member(user)
        for user in not_member_users:
            assert not project.has_member(user)
    
    @tag("project")
    def test_resource_time_allocated (self):
        """Sum a project's allocated time on a resource."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        user.can_request, user.can_allocate = True, True
        user.save()
        request = user.request(
            project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.save()
        assert request.project.resource_time_allocated(request.resource) == 0
        allocation = user.allocate(request,
            time = ALLOCATION_AMOUNT,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        allocation.save()
        assert request.project.resource_time_allocated(request.resource) == ALLOCATION_AMOUNT
    
    @tag("project")
    def test_resource_time_liened (self):
        """Sum a project's time liened on a resource."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        user.can_request, user.can_allocate, user.can_lien, user.can_charge = True, True, True, True
        user.save()
        request = user.request(
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.save()
        allocation = user.allocate(request,
            time = ALLOCATION_AMOUNT,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        allocation.save()
        assert request.project.resource_time_liened(request.resource) == 0
        lien = user.lien(
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.save()
        assert request.project.resource_time_liened(request.resource) == LIEN_AMOUNT
        charge = user.charge(lien, time=CHARGE_AMOUNT)
        charge.save()
        assert request.project.resource_time_liened(request.resource) == 0
    
    @tag("project")
    def test_resource_time_charged (self):
        """Sum a project's time charged for a resource."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        user.can_request, user.can_allocate, user.can_lien, user.can_charge, user.can_refund = True, True, True, True, True
        user.save()
        request = user.request(
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.save()
        allocation = user.allocate(request,
            time = ALLOCATION_AMOUNT,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        allocation.save()
        lien = user.lien(
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.save()
        assert request.project.resource_time_charged(request.resource) == 0
        charge = user.charge(lien, time=CHARGE_AMOUNT)
        charge.save()
        assert request.project.resource_time_charged(request.resource) == CHARGE_AMOUNT
        refund = user.refund(charge, time=REFUND_AMOUNT)
        refund.save()
        assert request.project.resource_time_charged(request.resource) == CHARGE_AMOUNT - REFUND_AMOUNT
    
    @tag("project")
    def test_resource_time_available (self):
        """Subtract a project's time liened and time charged from its time allocated."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        user.can_request, user.can_allocate, user.can_lien, user.can_charge, user.can_refund = True, True, True, True, True
        user.save()
        request = user.request(
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.save()
        assert request.project.resource_time_available(request.resource) == 0
        allocation = user.allocate(request,
            time = ALLOCATION_AMOUNT,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        allocation.save()
        assert request.project.resource_time_available(request.resource) == ALLOCATION_AMOUNT
        lien = user.lien(
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.save()
        assert request.project.resource_time_available(request.resource) == ALLOCATION_AMOUNT - LIEN_AMOUNT
        charge = user.charge(lien, time=CHARGE_AMOUNT)
        charge.save()
        assert request.project.resource_time_available(request.resource) == ALLOCATION_AMOUNT - CHARGE_AMOUNT
        refund = user.refund(charge, time=REFUND_AMOUNT)
        refund.save()
        assert request.project.resource_time_available(request.resource) == ALLOCATION_AMOUNT - (CHARGE_AMOUNT - REFUND_AMOUNT)
    
    @tag("project")
    def test_resource_credit_limit (self):
        """Determine the amount of credit available."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        user.can_allocate = True
        user.save()
        credit = user.allocate_credit(
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = CREDIT_LIMIT_AMOUNT,
            start = datetime.now() - timedelta(seconds=1),
        )
        assert credit.project.resource_credit_limit(credit.resource) == 0
        credit.save()
        assert credit.project.resource_credit_limit(credit.resource) == CREDIT_LIMIT_AMOUNT


class TestCreditLimit (TestModel):
    
    @tag("credit")
    def test_create (self):
        """Create a minimal credit limit."""
        credit = models.CreditLimit(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = CREDIT_LIMIT_AMOUNT,
        )
        
        # Fail without can_allocate.
        try:
            credit.save()
        except credit.poster.NotPermitted:
            pass
        else:
            assert False
        
        # Succeed with can_allocate.
        credit.poster.can_allocate = True
        try:
            credit.save()
        except:
            assert False
    
    @tag("credit")
    def test_negative (self):
        """Fail with a negative credit limit."""
        credit = models.CreditLimit(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = -1 * CREDIT_LIMIT_AMOUNT,
        )
        credit.poster.can_allocate = True
        try:
            credit.save()
        except ValueError:
            pass
        else:
            assert False
    

class TestRequest (TestModel):
    
    @tag("request")
    def test_create (self):
        """Create a minimal request limit."""
        member_project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0])
        not_member_project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['not_member_of'][0])
        
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = member_project,
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        
        # Request fails without can_request.
        request.poster.can_request = False
        try:
            request.save()
        except request.poster.NotPermitted:
            pass
        else:
            assert False
        
        # Request fails without membership.
        request.poster.can_request = True
        request.project = not_member_project
        try:
            request.save()
        except request.poster.NotAMember:
            pass
        else:
            assert False
        
        # Request succeeds if member and can_request.
        request.project = member_project
        try:
            request.save()
        except:
            assert False
    
    @tag("request")
    def test_allocate (self):
        """Allocate time for a request."""
        request = models.Request()
        allocation = request.allocate()
        assert isinstance(allocation, models.Allocation)
    
    @tag("request")
    def test_active (self):
        """Requests are active when they have not been allocated."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        assert request.active
        allocation = request.poster.allocate(request,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        allocation.poster.can_allocate = True
        allocation.save()
        assert not request.active


class TestAllocation (TestModel):
    
    @tag("allocation")
    def test_create (self):
        """Create a minimal allocation."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            time = ALLOCATION_AMOUNT,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
        )
        
        # Allocation fails without can_allocate.
        try:
            allocation.save()
        except allocation.poster.NotPermitted:
            pass
        else:
            assert False
        
        # Allocation succeeds with can_allocate.
        allocation.poster.can_allocate = True
        try:
            allocation.save()
        except:
            raise
            assert False
    
    @tag("allocation")
    def test_active (self):
        """Active when allocation starts before now and after last expiration."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        user.can_request, user.can_allocate = True, True
        user.save()
        project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name'])
        resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name'])
        request = models.Request(
            poster = user,
            project = project,
            resource = resource,
            time = REQUEST_AMOUNT,
            start = datetime.now(),
        )
        request.save()
        allocation = models.Allocation(
            poster = user,
            request = request,
            time = ALLOCATION_AMOUNT,
            start = datetime.now() + timedelta(days=1),
            expiration = datetime.now() + timedelta(days=2),
        )
        allocation.save()
        
        # Allocation has not yet started.
        assert not allocation.active
        
        # Allocation has started but expired.
        allocation.start = datetime.now() - timedelta(days=2)
        allocation.expiration = datetime.now() - timedelta(days=1)
        allocation.save()
        assert not allocation.active
        
        # Allocation has started and not expired.
        allocation.expiration = datetime.now() + timedelta(days=1)
        assert allocation.active
    
    @tag("allocation")
    def test_convenience_properties (self):
        """Allocation project and resource point back to request."""
        user = models.User.from_upstream_name(UPSTREAM_TEST_USER['name'])
        user.can_request, user.can_allocate = True, True
        user.save()
        project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name'])
        resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name'])
        request = models.Request(
            poster = user,
            project = project,
            resource = resource,
            time = REQUEST_AMOUNT,
            start = datetime.now(),
        )
        request.save()
        allocation = models.Allocation(
            request = request,
        )
        assert allocation.project == request.project
        assert allocation.resource == request.resource


class TestLien (TestModel):
    
    @tag("lien")
    def test_minimal (self):
        """Create a minimal lien."""
        member_user = models.User.from_upstream_name(UPSTREAM_TEST_PROJECT['has_member'][0])
        not_member_user = models.User.from_upstream_name(UPSTREAM_TEST_PROJECT['not_has_member'][0])
        
        request = models.Request(
            poster = member_user,
            project = models.Project.from_upstream_name(UPSTREAM_TEST_PROJECT['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = member_user,
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        
        # Fail without can_lien.
        try:
            lien.save()
        except lien.poster.NotPermitted:
            pass
        else:
            assert False
        
        # Fail without membership.
        lien.poster = not_member_user
        lien.poster.can_lien = True
        try:
            lien.save()
        except lien.poster.NotAMember:
            pass
        else:
            assert False
        
        # Succeed with membership and can_lien.
        lien.poster = member_user
        lien.poster.can_lien = True
        try:
            lien.save()
        except:
            raise
            assert False
    
    @tag("lien")
    def test_convenience_properties (self):
        """Convenience properties for project and resource."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        assert lien.project == request.project
        assert lien.resource == request.resource
    
    @tag("lien")
    def test_effective_charge (self):
        """Sum the effective charges of all charges."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.poster.can_lien = True
        lien.save()
        assert lien.effective_charge == 0
        charge = lien.poster.charge(lien, time=CHARGE_AMOUNT)
        charge.poster.can_charge = True
        charge.save()
        assert lien.effective_charge == CHARGE_AMOUNT
        refund = allocation.poster.refund(charge, time=REFUND_AMOUNT)
        refund.poster.can_refund = True
        refund.save()
        assert lien.effective_charge == CHARGE_AMOUNT - REFUND_AMOUNT
    
    @tag("lien")
    def test_time_available (self):
        """Subtract the sum of a lien's charges from its time."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.poster.can_lien = True
        lien.save()
        assert lien.time_available == LIEN_AMOUNT
        charge = lien.poster.charge(lien, time=CHARGE_AMOUNT)
        charge.poster.can_charge = True
        charge.save()
        assert lien.time_available == LIEN_AMOUNT - CHARGE_AMOUNT
        refund = allocation.poster.refund(charge, time=REFUND_AMOUNT)
        refund.poster.can_refund = True
        refund.save()
        assert lien.time_available == LIEN_AMOUNT - (CHARGE_AMOUNT - REFUND_AMOUNT)
    
    @tag("lien")
    def test_lien_not_negative (self):
        """Liens cannot have negative time."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = -1 * LIEN_AMOUNT,
        )
        lien.poster.can_lien = True
        try:
            lien.save()
        except ValueError:
            pass
        else:
            assert False
    
    @tag("lien")
    def test_charge (self):
        """Charge the lien."""
        lien = models.Lien()
        charge = lien.charge()
        assert isinstance(charge, models.Charge)
    
    @tag("lien")
    def test_active (self):
        """A lien is active when its allocation is active."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now() + timedelta(days=1),
            expiration = datetime.now() + timedelta(days=2),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
            datetime = datetime.now() - timedelta(days=2),
        )
        lien.poster.can_lien = True
        lien.save()
        
        # Allocation has not started.
        assert not lien.active
        
        # Allocation has started, but expired.
        lien.allocation.start = datetime.now() - timedelta(days=2)
        lien.allocation.expiration = datetime.now() - timedelta(days=1)
        lien.save()
        assert not lien.active
        
        # Allocation has started and not yet expired.
        lien.allocation.expiration = datetime.now() + timedelta(days=1)
        assert lien.active
    
    @tag("lien")
    def test_open (self):
        """A lien is open when uncharged."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now() - timedelta(days=1),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
            datetime = datetime.now(),
        )
        lien.poster.can_lien = True
        lien.save()
        
        # Lien has not been charged.
        assert lien.open
        
        # Lien has been charged.
        charge = lien.poster.charge(lien, time=CHARGE_AMOUNT)
        charge.poster.can_charge = True
        charge.save()
        assert not lien.open


class TestCharge (TestModel):
    
    @tag("charge")
    def test_create (self):
        """Create a minimal charge."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.poster.can_lien = True
        lien.save()
        
        charge = models.Charge(
            lien = lien,
            poster = lien.poster,
            time = CHARGE_AMOUNT,
        )
        
        # Charge fails without can_charge.
        try:
            charge.save()
        except charge.poster.NotPermitted:
            pass
        else:
            assert False
        
        # Charge succeeds with can_charge.
        charge.poster.can_charge = True
        try:
            charge.save()
        except:
            assert False
    
    @tag("charge")
    def test_negative_time (self):
        """Charges cannot have negative time."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.poster.can_lien = True
        lien.save()
        charge = models.Charge(
            lien = lien,
            poster = lien.poster,
            time = -1 * CHARGE_AMOUNT,
        )
        charge.poster.can_charge = True
        try:
            charge.save()
        except ValueError:
            pass
        else:
            assert False
    
    @tag("charge")
    def test_effective_charge (self):
        """Subtract the sum of a charge's refunds from the charge's time."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.poster.can_lien = True
        lien.save()
        charge = models.Charge(
            lien = lien,
            poster = lien.poster,
            time = CHARGE_AMOUNT,
        )
        charge.poster.can_charge = True
        charge.save()
        assert charge.effective_charge == CHARGE_AMOUNT
        refund = charge.poster.refund(charge, time=REFUND_AMOUNT)
        refund.poster.can_refund = True
        refund.save()
        assert charge.effective_charge == CHARGE_AMOUNT - REFUND_AMOUNT
    
    @tag("charge")
    def test_convenience_properties (self):
        """Convenience properties for project and resource."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.poster.can_lien = True
        lien.save()
        charge = models.Charge(
            lien = lien,
            poster = lien.poster,
            time = CHARGE_AMOUNT,
        )
        assert charge.project == lien.project
        assert charge.resource == lien.resource
    
    @tag("charge")
    def test_refund (self):
        """Refund time from a charge."""
        charge = models.Charge()
        refund = charge.refund()
        assert isinstance(refund, models.Refund)


class TestRefund (TestModel):
    
    @tag("refund")
    def test_create (self):
        """A minimal refund."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.poster.can_lien = True
        lien.save()
        charge = models.Charge(
            lien = lien,
            poster = lien.poster,
            time = CHARGE_AMOUNT,
        )
        charge.poster.can_charge = True
        charge.save()
        
        refund = models.Refund(
            poster = charge.poster,
            charge = charge,
            time = REFUND_AMOUNT,
        )
        # Fail without can_refund
        try:
            refund.save()
        except refund.poster.NotPermitted:
            pass
        else:
            assert False
        
        # Succeed.
        refund.poster.can_refund = True
        try:
            refund.save()
        except:
            assert False
    
    @tag("refund")
    def test_convenience_properties (self):
        """Convenience properties for project and resource."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.poster.can_lien = True
        lien.save()
        charge = models.Charge(
            lien = lien,
            poster = lien.poster,
            time = CHARGE_AMOUNT,
        )
        charge.poster.can_charge = True
        charge.save()
        refund = models.Refund(
            charge = charge,
        )
        assert refund.project == refund.charge.lien.project
        assert refund.resource == refund.charge.lien.resource
    
    @tag("refund")
    def test_negative_time (self):
        """Fail when refunding negative time."""
        request = models.Request(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            project = models.Project.from_upstream_name(UPSTREAM_TEST_USER['member_of'][0]),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            time = REQUEST_AMOUNT,
        )
        request.poster.can_request = True
        request.save()
        allocation = models.Allocation(
            request = request,
            poster = request.poster,
            start = datetime.now(),
            expiration = datetime.now() + timedelta(days=1),
            time = ALLOCATION_AMOUNT,
        )
        allocation.poster.can_allocate = True
        allocation.save()
        lien = models.Lien(
            poster = request.poster,
            allocation = allocation,
            time = LIEN_AMOUNT,
        )
        lien.poster.can_lien = True
        lien.save()
        charge = models.Charge(
            lien = lien,
            poster = lien.poster,
            time = CHARGE_AMOUNT,
        )
        charge.poster.can_charge = True
        charge.save()
        refund = models.Refund(
            poster = charge.poster,
            charge = charge,
            time = -1 * REFUND_AMOUNT,
        )
        refund.poster.can_refund = True
        try:
            refund.save()
        except ValueError:
            pass
        else:
            assert False


class TestUnitFactor (TestModel):
    
    @tag("factor")
    def test_create (self):
        """Create a standard factor."""
        factor = models.UnitFactor(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            factor = UNIT_FACTOR,
        )
        try:
            factor.save()
        except:
            assert False
    
    @tag("factor")
    def test_resource_factor (self):
        """Retrieve a factor for a resource."""
        factor = factor = models.UnitFactor(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            factor = UNIT_FACTOR,
        )
        factor.save()
        jitter = models.UnitFactor.resource_factor(factor.resource) - factor.factor
        assert jitter < 0.1
    
    @tag("factor")
    def test_to_ru (self):
        """Convert from standard units to resource units."""
        factor = factor = models.UnitFactor(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            factor = UNIT_FACTOR,
        )
        factor.save()
        resource_units = models.UnitFactor.to_ru(factor.resource, STANDARD_UNITS)
        assert resource_units == RESOURCE_UNITS
    
    @tag("factor")
    def test_to_su (self):
        """Convert from resource units to standard units."""
        factor = factor = models.UnitFactor(
            poster = models.User.from_upstream_name(UPSTREAM_TEST_USER['name']),
            resource = models.Resource.from_upstream_name(UPSTREAM_TEST_RESOURCE['name']),
            factor = UNIT_FACTOR,
        )
        factor.save()
        standard_units = models.UnitFactor.to_su(factor.resource, RESOURCE_UNITS)
        assert standard_units == STANDARD_UNITS
