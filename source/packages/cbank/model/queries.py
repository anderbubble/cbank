from datetime import datetime

from sqlalchemy.sql import func, and_, case
from sqlalchemy.orm import scoped_session, sessionmaker, joinedload
from sqlalchemy.orm.session import SessionExtension
from sqlalchemy.orm.exc import NoResultFound

from cbank.model import (
    User, Project,
    Allocation, Hold, Job, Charge, Refund)
from cbank.model.entities import parse_pbs


__all__ = [
    "Session", "get_projects", "get_users", "import_job",
    "user_summary", "project_summary", "allocation_summary",
    "hold_summary", "charge_summary"]


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
        holds_ = (
            instance for instance in (session.new | session.dirty)
            if isinstance(instance, Hold) and instance.active)
        for allocation in set(hold.allocation for hold in holds_):
            amount_used = (
                allocation.amount_charged() + allocation.amount_held())
            if amount_used > allocation.amount:
                raise ValueError("cannot hold more than is available")
    
    def check_refunds (self, session):
        """Require new refunds to fit in their charges."""
        refunds_ = (
            instance for instance in (session.new | session.dirty)
            if isinstance(instance, Refund))
        charges_ = set(refund.charge for refund in refunds_)
        for charge in charges_:
            if charge.effective_amount() < 0:
                raise ValueError("cannot refund more than was charged")
    
    def before_commit (self, session):
        """Check constraints before committing."""
        self.check_amounts(session)
        self.check_holds(session)
        self.check_refunds(session)


Session = scoped_session(sessionmaker(extension=EntityConstraints()))


def get_projects (member=None, manager=None):
    """Allocated projects that have a given member or manager."""
    projects = (
        Project.cached(project_id)
        for (project_id, ) in Session.query(Allocation.project_id).distinct())
    if member:
        projects = (
            project for project in projects
            if member.is_member(project))
    if manager:
        projects = (
            project for project in projects if manager.is_manager(project))
    return list(projects)


def get_users (member=None, manager=None):
    users = (
        User.cached(user_id) for (user_id, )
        in Session.query(Job.user_id).distinct())
    if member:
        users = (
            user for user in users
            if user.is_member(member))
    if manager:
        users = (
            user for user in users
            if user.is_manager(manager))
    return list(users)


def import_job (entry):
    _, id_, _ = parse_pbs(entry)
    try:
        job = Session.query(Job).filter_by(id=id_).one()
    except NoResultFound:
        job = Job.from_pbs(entry)
    else:
        job.update_from_pbs(entry)
    return job


def user_summary (users, projects=None, resources=None,
                  after=None, before=None):
    s = Session()
    jobs_q = s.query(
        Job.user_id.label("user_id"),
        func.count(Job.id).label("job_count"))
    charges_q = s.query(
        Job.user_id.label("user_id"),
        func.sum(Charge.amount).label("charge_sum"))
    charges_q = charges_q.join(Charge.job)
    refunds_q = s.query(
        Job.user_id.label("user_id"),
        func.sum(Refund.amount).label("refund_sum"))
    refunds_q = refunds_q.join(Refund.charge, Charge.job)

    jobs_q.filter(Job.user_id.in_(user.id for user in users))
    if projects:
        jobs_q = jobs_q.filter(
            Job.account_id.in_(project.id for project in projects))
        charges_ = Charge.allocation.has(
            Allocation.project_id.in_(project.id for project in projects))
        charges_q = charges_q.filter(charges_)
        refunds_q = refunds_q.filter(charges_)
    if resources:
        charges_ = Charge.allocation.has(Allocation.resource_id.in_(
            resource.id for resource in resources))
        jobs_q = jobs_q.filter(Job.charges.any(charges_))
        charges_q = charges_q.filter(charges_)
        refunds_q = refunds_q.filter(charges_)
    if after:
        jobs_q = jobs_q.filter(Job.end > after)
        charges_ = Charge.datetime >= after
        charges_q = charges_q.filter(charges_)
        refunds_q = refunds_q.filter(charges_)
    if before:
        jobs_q = jobs_q.filter(Job.start < before)
        charges_ = Charge.datetime < before
        charges_q = charges_q.filter(charges_)
        refunds_q = refunds_q.filter(charges_)

    jobs_q = jobs_q.group_by(Job.user_id).subquery()
    charges_q = charges_q.group_by(Job.user_id).subquery()
    refunds_q = refunds_q.group_by(Job.user_id).subquery()
    query = s.query(
        Job.user_id,
        func.coalesce(jobs_q.c.job_count, 0),
        (func.coalesce(charges_q.c.charge_sum, 0)
            - func.coalesce(refunds_q.c.refund_sum, 0)))
    query = query.outerjoin(
        (jobs_q, Job.user_id == jobs_q.c.user_id),
        (charges_q, Job.user_id == charges_q.c.user_id),
        (refunds_q, Job.user_id == refunds_q.c.user_id))
    query = query.filter(Job.user_id.in_(user.id for user in users))
    query = query.distinct().order_by(Job.user_id)
    return query


def project_summary (projects, users=None, resources=None,
                     before=None, after=None):
    now = datetime.now()
    s = Session()
    allocations_q = s.query(
        Allocation.project_id,
        func.sum(Allocation.amount).label("allocation_sum")
        ).group_by(Allocation.project_id)

    holds_q = s.query(
        Allocation.project_id,
        func.sum(Hold.amount).label("hold_sum"))
    holds_q = holds_q.group_by(Allocation.project_id)
    holds_q = holds_q.join(Hold.allocation)
    holds_q = holds_q.filter(Hold.active == True)
    jobs_q = s.query(
        Allocation.project_id,
        func.count(Job.id).label("job_count")).group_by(Allocation.project_id)
    jobs_q = jobs_q.outerjoin(Job.charges, Charge.allocation)
    charges_q = s.query(
        Allocation.project_id,
        func.sum(Charge.amount).label("charge_sum"))
    charges_q = charges_q.group_by(Allocation.project_id)
    charges_q = charges_q.join(Charge.allocation)
    refunds_q = s.query(
        Allocation.project_id,
        func.sum(Refund.amount).label("refund_sum"))
    refunds_q = refunds_q.group_by(Allocation.project_id)
    refunds_q = refunds_q.join(Refund.charge, Charge.allocation)

    if resources:
        resources_ = Allocation.resource_id.in_(
            resource.id for resource in resources)
        allocations_q = allocations_q.filter(resources_)
        holds_q = holds_q.filter(resources_)
        charges_q = charges_q.filter(resources_)
        refunds_q = refunds_q.filter(resources_)
        jobs_q = jobs_q.filter(resources_)

    allocations_active = and_(Allocation.start <= now, Allocation.end > now)
    allocations_q = allocations_q.filter(allocations_active)
    holds_q = holds_q.filter(Hold.allocation.has(allocations_active))
    charges_ = Charge.allocation.has(allocations_active)
    balance_charges_q = charges_q.filter(charges_)
    balance_refunds_q = refunds_q.filter(Refund.charge.has(charges_))

    holds_q = holds_q.subquery()
    balance_charges_q = balance_charges_q.subquery()
    balance_refunds_q = balance_refunds_q.subquery()

    if users:
        users_ = Job.user_id.in_(user.id for user in users)
        jobs_q = jobs_q.filter(users_)
        charges_ = Charge.job.has(users_)
        charges_q = charges_q.filter(charges_)
        refunds_q = refunds_q.filter(charges_)
    if after:
        jobs_q = jobs_q.filter(Job.end > after)
        after_ = Charge.datetime >= after
        charges_q = charges_q.filter(after_)
        refunds_q = refunds_q.filter(after_)
    if before:
        jobs_q = jobs_q.filter(Job.start < before)
        before_ = Charge.datetime < before
        charges_q = charges_q.filter(before_)
        refunds_q = refunds_q.filter(before_)

    allocations_q = allocations_q.subquery()
    jobs_q = jobs_q.subquery()
    charges_q = charges_q.subquery()
    refunds_q = refunds_q.subquery()

    balance = (
        func.coalesce(allocations_q.c.allocation_sum, 0)
        - func.coalesce(holds_q.c.hold_sum, 0)
        - func.coalesce(balance_charges_q.c.charge_sum, 0)
        + func.coalesce(balance_refunds_q.c.refund_sum, 0))

    query = s.query(
        Allocation.project_id,
        func.coalesce(jobs_q.c.job_count, 0),
        (func.coalesce(charges_q.c.charge_sum, 0)
         - func.coalesce(refunds_q.c.refund_sum, 0)),
        case([(balance>=0, balance)], else_=0))
    query = query.distinct()
    query = query.outerjoin(
        (jobs_q, Allocation.project_id == jobs_q.c.project_id),
        (charges_q, Allocation.project_id == charges_q.c.project_id),
        (refunds_q, Allocation.project_id == refunds_q.c.project_id),
        (allocations_q, Allocation.project_id == allocations_q.c.project_id),
        (holds_q, Allocation.project_id == holds_q.c.project_id),
        (balance_charges_q,
            Allocation.project_id == balance_charges_q.c.project_id),
        (balance_refunds_q,
            Allocation.project_id == balance_refunds_q.c.project_id))
    query = query.order_by(Allocation.project_id)
    query = query.filter(
        Allocation.project_id.in_(project.id for project in projects))
    return query


def allocation_summary (allocations, users=None,
                        before=None, after=None):
    now = datetime.now()
    s = Session()
    holds_q = s.query(
        Allocation.id.label("allocation_id"),
        func.sum(Hold.amount).label("hold_sum")).group_by(Allocation.id)
    holds_q = holds_q.join(Hold.allocation)
    holds_q = holds_q.filter(Hold.active == True)
    charges_q = s.query(
        Allocation.id.label("allocation_id"),
        func.sum(Charge.amount).label("charge_sum")).group_by(Allocation.id)
    charges_q = charges_q.join(Charge.allocation)
    refunds_q = s.query(
        Allocation.id.label("allocation_id"),
        func.sum(Refund.amount).label("refund_sum")).group_by(Allocation.id)
    refunds_q = refunds_q.join(
        Refund.charge, Charge.allocation)
    jobs_q = s.query(
        Allocation.id.label("allocation_id"),
        func.count(Job.id).label("job_count")).group_by(Allocation.id)
    jobs_q = jobs_q.join(
        Job.charges, Charge.allocation)

    balance_charges_q = charges_q.subquery()
    balance_refunds_q = refunds_q.subquery()
    holds_q = holds_q.subquery()

    if users:
        jobs_ = Job.user_id.in_(str(user.id) for user in users)
        jobs_q = jobs_q.filter(jobs_)
        charges_ = Charge.job.has(jobs_)
        charges_q = charges_q.filter(charges_)
        refunds_q = refunds_q.filter(charges_)
    if after:
        jobs_q = jobs_q.filter(Job.end > after)
        after_ = Charge.datetime >= after
        charges_q = charges_q.filter(after_)
        refunds_q = refunds_q.filter(after_)
    if before:
        jobs_q = jobs_q.filter(Job.start < before)
        before_ = Charge.datetime < before
        charges_q = charges_q.filter(before_)
        refunds_q = refunds_q.filter(before_)

    charges_q = charges_q.subquery()
    refunds_q = refunds_q.subquery()
    jobs_q = jobs_q.subquery()

    balance = (
        Allocation.amount
        - func.coalesce(holds_q.c.hold_sum, 0)
        - func.coalesce(balance_charges_q.c.charge_sum, 0)
        + func.coalesce(balance_refunds_q.c.refund_sum, 0))

    allocations_active = and_(Allocation.start <= now, Allocation.end > now)

    query = s.query(
        Allocation,
        func.coalesce(jobs_q.c.job_count, 0),
        (func.coalesce(charges_q.c.charge_sum, 0)
            - func.coalesce(refunds_q.c.refund_sum, 0)),
        (case([(and_(allocations_active, balance>=0), balance)], else_=0)))
    query = query.outerjoin(
        (jobs_q, Allocation.id == jobs_q.c.allocation_id),
        (charges_q, Allocation.id == charges_q.c.allocation_id),
        (balance_charges_q, Allocation.id == balance_charges_q.c.allocation_id),
        (refunds_q, Allocation.id == refunds_q.c.allocation_id),
        (balance_refunds_q, Allocation.id == balance_refunds_q.c.allocation_id),
        (holds_q, Allocation.id == holds_q.c.allocation_id))
    query = query.order_by(Allocation.id)
    query = query.filter(Allocation.id.in_(
            allocation.id for allocation in allocations))
    return query


def hold_summary (users=None, projects=None, resources=None, jobs=None, after=None, before=None):
    s = Session()
    query = s.query(Hold).filter_by(active=True)
    query = query.options(joinedload(Hold.allocation))
    query = query.order_by(Hold.datetime, Hold.id)

    if users:
        query = query.filter(Hold.job.has(Job.user_id.in_(
            user.id for user in users)))
    if projects:
        query = query.filter(Hold.allocation.has(
            Allocation.project_id.in_(project.id for project in projects)))
    if resources:
        query = query.filter(Hold.allocation.has(
            Allocation.resource_id.in_(resource.id for resource in resources)))
    if after:
        query = query.filter(Hold.datetime >= after)
    if before:
        query = query.filter(Hold.datetime < before)
    if jobs:
        query = query.filter(Hold.job.has(Job.id.in_(
            job.id for job in jobs)))

    return query


def charge_summary (users=None, projects=None, resources=None, jobs=None, after=None, before=None):
    s = Session()
    query = s.query(Charge)
    query = query.options(joinedload(Charge.allocation))
    query = query.order_by(Charge.datetime, Charge.id)

    if users:
        query = query.filter(Charge.job.has(Job.user_id.in_(
            user.id for user in users)))
    if projects:
        query = query.filter(Charge.allocation.has(
            Allocation.project_id.in_(project.id for project in projects)))
    if resources:
        query = query.filter(Charge.allocation.has(
            Allocation.resource_id.in_(resource.id for resource in resources)))
    if after:
        query = query.filter(Charge.datetime >= after)
    if before:
        query = query.filter(Charge.datetime < before)
    if jobs:
        query = query.filter(Charge.job.has(Job.id.in_(
            job.id for job in jobs)))

    return query
