from cbank.model import (
    User, Project,
    Allocation, Hold, Job, Charge, Refund)
from cbank.model.entities import parse_pbs

from sqlalchemy.sql import func
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm.session import SessionExtension
from sqlalchemy.orm.exc import NoResultFound


__all__ = ["Session", "get_projects", "get_users", "import_job"]


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
            if isinstance(instance, Hold) and instance.active)
        for allocation in set(hold.allocation for hold in holds_):
            if allocation.amount_available() < 0:
                raise ValueError("cannot hold more than is available")
    
    def check_refunds (self, session):
        """Require new refunds to fit in their charges."""
        refunds_ = (instance for instance in (session.new | session.dirty)
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
        Project.cached(project_id) for (project_id, )
        in Session.query(Allocation.project_id).distinct())
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
