import os
import sys
import locale
import ConfigParser
import string
import warnings
from datetime import datetime
try:
    set
except NameError:
    from sets import Set as set

import sqlalchemy.sql as sql
import sqlalchemy.types as types

from clusterbank import config
from clusterbank.cbank.common import get_unit_factor
import clusterbank.model as model

__all__ = ["unit_definition", "convert_units", "display_units",
    "print_users_report", "print_projects_report", "print_allocations_report",
    "print_holds_report", "print_charges_report"]

locale.setlocale(locale.LC_ALL, locale.getdefaultlocale()[0])


class IntSum (types.TypeDecorator):
    impl = types.String
    
    def process_bind_param (self, value, dialect):
        return value
    
    def process_result_value (self, value, dialect):
        try:
            return int(value)
        except TypeError:
            if value is None:
                return 0
            else:
                return value


def print_users_report (**kwargs):
    
    """Users report.
    
    The users report lists the number of charges and total amount charged
    for each user in the system.
    
    Keyword arguments:
    projects -- require project membership and charge relationship
    """
    
    format = Formatter(["User", "Charges", "Charged"])
    format.widths = {'User':10, 'Charges':8, 'Charged':15}
    format.aligns = {'Charges':"right", 'Charged':"right"}
    print format.header()
    print format.bar()
    
    s = model.Session()
    
    correlation = model.charges.c.user_id==model.users.c.id
    charges_count_query = sql.select([
        sql.func.count(model.charges.c.id)], correlation,
        from_obj=model.charges.join(model.allocations))
    charges_sum_query = sql.select([
        sql.func.sum(model.charges.c.amount, type_=IntSum)], correlation,
        from_obj=model.charges.join(model.allocations))
    refunds_sum_query = sql.select([
        sql.func.sum(model.refunds.c.amount, type_=IntSum)],
        correlation, from_obj=model.refunds.join(
        model.charges).join(model.allocations))
    
    conditions = []
    if kwargs.get("projects"):
        conditions.append(model.allocations.c.project_id.in_(
                project.id for project in kwargs.get("projects")))
    if kwargs.get("resources"):
        conditions.append(model.allocations.c.resource_id.in_(
                resource.id for resource in kwargs.get("resources")))
    if kwargs.get("after"):
        conditions.append(model.charges.c.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        conditions.append(model.charges.c.datetime<kwargs.get("before"))
    if conditions:
        conditions = sql.and_(*conditions)
        charges_count_query = charges_count_query.where(conditions)
        charges_sum_query = charges_sum_query.where(conditions)
        refunds_sum_query = refunds_sum_query.where(conditions)
    
    query = sql.select([
        model.users.c.id,
        charges_count_query.label("charges_count"),
        charges_sum_query.label("charges_sum"),
        refunds_sum_query.label("refunds_sum"),
        ]).select_from(model.users)
    
    if kwargs.get("users"):
        query = query.where(model.users.c.id.in_(
            user.id for user in kwargs.get("users")))
    
    total_charges = 0
    total_charged = 0
    for row in s.execute(query):
        user = model.upstream.get_user_name(row[model.users.c.id])
        charges = row['charges_count']
        charged = row['charges_sum'] - row['refunds_sum']
        total_charges += charges
        total_charged += charged
        print format({'User':user, 'Charges':charges,
            'Charged':display_units(charged)})
    print format.bar(["Charges", "Charged"])
    print format({'Charges':total_charges,
        'Charged':display_units(total_charged)})
    print unit_definition()

def print_projects_report (**kwargs):
    
    """Projects report.
    
    The projects report lists allocations and charges for each project
    in the system.
    """
    
    format = Formatter([
        "Project", "Available", "Charges", "Charged"])
    format.widths = {'Project':15, 'Charges':7, 'Charged':15, 'Available':15}
    format.aligns = {'Charges':"right",
        'Charged':"right", "Available":"right"}
    print format.header()
    print format.bar()
    
    s = model.Session()
    now = datetime.now()
    
    correlation = \
        model.allocations.c.project_id==model.projects.c.id
    allocations_sum_query = sql.select([
        sql.func.sum(model.allocations.c.amount, type_=IntSum)],
        correlation, from_obj=model.allocations)
    holds_sum_query = sql.select([
        sql.func.sum(model.holds.c.amount, type_=IntSum)],
        correlation, from_obj=model.holds.join(
            model.allocations)).where(model.holds.c.active==True)
    charges_sum_query = sql.select([
        sql.func.sum(model.charges.c.amount, type_=IntSum)],
        correlation, from_obj=model.charges.join(
            model.allocations))
    refunds_sum_query = sql.select([
        sql.func.sum(model.refunds.c.amount, type_=IntSum)],
        correlation, from_obj=model.refunds.join(
            model.charges).join(model.allocations))
    m_charges_count_query = sql.select([
        sql.func.count(model.charges.c.amount)], correlation,
            from_obj=model.charges.join(model.allocations))
    m_charges_sum_query = charges_sum_query
    m_refunds_sum_query = refunds_sum_query
    
    conditions = [sql.and_(model.allocations.c.start<=now,
            model.allocations.c.expiration>now)]
    m_conditions = []
    if kwargs.get("users"):
        m_conditions.append(model.charges.c.user_id.in_(
                user.id for user in kwargs.get("users")))
    if kwargs.get("resources"):
        _condition = model.allocations.c.resource_id.in_(
            resource.id for resource in kwargs.get("resources"))
        conditions.append(_condition)
        m_conditions.append(_condition)
    if kwargs.get("before"):
        m_conditions.append(model.charges.c.datetime<kwargs.get("before"))
    if kwargs.get("after"):
        m_conditions.append(model.charges.c.datetime>=kwargs.get("after"))
    if conditions:
        conditions = sql.and_(*conditions)
        allocations_sum_query = allocations_sum_query.where(conditions)
        holds_sum_query = holds_sum_query.where(conditions)
        charges_sum_query = charges_sum_query.where(conditions)
        refunds_sum_query = refunds_sum_query.where(conditions)
    if m_conditions:
        m_conditions = sql.and_(*m_conditions)
        m_charges_count_query = m_charges_count_query.where(m_conditions)
        m_charges_sum_query = m_charges_sum_query.where(m_conditions)
        m_refunds_sum_query = m_refunds_sum_query.where(m_conditions)
    
    query = sql.select([
        model.projects.c.id,
        allocations_sum_query.label("allocations_sum"),
        holds_sum_query.label("holds_sum"),
        charges_sum_query.label("charges_sum"),
        refunds_sum_query.label("refunds_sum"),
        m_charges_count_query.label("m_charges_count"),
        m_charges_sum_query.label("m_charges_sum"),
        m_refunds_sum_query.label("m_refunds_sum")])
    
    query = query.select_from(model.projects)
    
    if kwargs.get("projects"):
        query = query.where(model.projects.c.id.in_(
            project.id for project in kwargs.get("projects")))
    
    total_available = 0
    total_charges = 0
    total_charged = 0
    for row in s.execute(query):
        project = \
            model.upstream.get_project_name(row[model.projects.c.id])
        available = row['allocations_sum'] \
            - (row['holds_sum'] + (row['charges_sum'] - row['refunds_sum']))
        total_available += available
        charges = row['m_charges_count']
        total_charges += charges
        charged = row['m_charges_sum'] - row['m_refunds_sum']
        total_charged += charged
        print format({'Project':project,
            'Available':display_units(available),
            'Charges':charges, 'Charged':display_units(charged)})
    print format.bar(["Charges", "Charged", "Available"])
    print format({'Charges':total_charges,
        'Charged':display_units(total_charged),
        'Available':display_units(total_available)})
    print unit_definition()

def print_allocations_report (**kwargs):
    
    """Allocations report.
    
    The projects report lists attributes of and charges against allocations
    in the system.
    """
    
    if kwargs.get("comments"):
        format = Formatter(["Allocation", "Project", "Resource", "Expiration",
            "Available", "Charges", "Charged", "Comment"])
    else:
        format = Formatter(["Allocation", "Project", "Resource", "Expiration",
            "Available", "Charges", "Charged"])
    format.headers = {'Allocation':"#"}
    format.widths = {'Allocation':4, 'Project':15, 'Available':13,
        'Charges':7, 'Charged':13, 'Expiration':10}
    format.aligns = {'Available':"right", 'Charges':"right", 'Charged':"right"}
    print format.header()
    print format.bar()
    
    s = model.Session()
    now = datetime.now()
    
    correlation = \
        model.charges.c.allocation_id==model.allocations.c.id
    charges_sum_query = sql.select([
        sql.func.sum(model.charges.c.amount, type_=IntSum)],
        correlation)
    refunds_sum_query = sql.select([
        sql.func.sum(model.refunds.c.amount, type_=IntSum)],
        correlation, from_obj=model.refunds.join(model.charges))
    holds_sum_query = sql.select([
        sql.func.sum(model.holds.c.amount, type_=IntSum)],
        correlation).where(model.holds.c.active==True)
    m_charges_count_query = sql.select([
        sql.func.count(model.charges.c.id)], correlation)
    m_charges_sum_query = charges_sum_query
    m_refunds_sum_query = refunds_sum_query
    
    m_conditions = []
    if kwargs.get("users"):
        m_conditions.append(model.charges.c.user_id.in_(
            user.id for user in kwargs.get("users")))
    if kwargs.get("after"):
        m_conditions.append(model.charges.c.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        m_conditions.append(model.charges.c.datetime<kwargs.get("before"))
    if m_conditions:
        m_conditions = sql.and_(*m_conditions)
        m_charges_count_query = m_charges_count_query.where(m_conditions)
        m_charges_sum_query = m_charges_sum_query.where(m_conditions)
        m_refunds_sum_query = m_refunds_sum_query.where(m_conditions)
    
    query = sql.select([
        model.allocations.c.id,
        model.projects.c.id,
        model.resources.c.id,
        model.allocations.c.expiration,
        model.allocations.c.amount,
        model.allocations.c.comment,
        holds_sum_query.label("holds_sum"),
        charges_sum_query.label("charges_sum"),
        refunds_sum_query.label("refunds_sum"),
        m_charges_count_query.label("m_charges_count"),
        m_charges_sum_query.label("m_charges_sum"),
        m_refunds_sum_query.label("m_refunds_sum"),
        ], use_labels=True)
    
    query = query.select_from(model.allocations.join(
        model.projects).join(model.resources))
    
    if kwargs.get("projects"):
        query = query.where(model.projects.c.id.in_(
            project.id for project in kwargs.get("projects")))
    if kwargs.get("resources"):
        query = query.where(model.resources.c.id.in_(
            resource.id for resource in kwargs.get("resources")))
    if kwargs.get("after") or kwargs.get("before"):
        if kwargs.get("after"):
            query = query.where(
                model.allocations.c.expiration>kwargs.get("after"))
        if kwargs.get("before"):
            query = query.where(
                model.allocations.c.start<=kwargs.get("before"))
    else:
        query = query.where(sql.and_(model.allocations.c.start<=now,
            model.allocations.c.expiration>now))
    
    total_available = 0
    total_charges = 0
    total_charged = 0
    for row in s.execute(query):
        allocation = row[model.allocations.c.id]
        comment = row[model.allocations.c.comment]
        project = model.upstream.get_project_name(
            row[model.projects.c.id])
        resource = model.upstream.get_resource_name(
            row[model.resources.c.id])
        expiration = format_datetime(
            row[model.allocations.c.expiration])
        charges = row['m_charges_count']
        total_charges += charges
        available = row[model.allocations.c.amount] \
            - (row['holds_sum'] + (row['charges_sum'] - row['refunds_sum']))
        total_available += available
        charged = row['m_charges_sum'] - row['m_refunds_sum']
        total_charged += charged
        if kwargs.get("comments"):
            print format({
                'Allocation':allocation,
                'Project':project,
                'Resource':resource,
                'Expiration':expiration,
                'Available':display_units(available),
                'Charges':charges,
                'Charged':display_units(charged),
                'Comment':comment})
        else:
            print format({
                'Allocation':allocation,
                'Project':project,
                'Resource':resource,
                'Expiration':expiration,
                'Available':display_units(available),
                'Charges':charges,
                'Charged':display_units(charged)})
    print format.bar(["Available", "Charges", "Charged"])
    print format({
        'Available':display_units(total_available),
        'Charges':total_charges,
        'Charged':display_units(total_charged)})
    print unit_definition()

def print_holds_report (**kwargs):
    
    """Holds report.
    
    The holds report displays individual holds.
    """
    
    if kwargs.get("comments"):
        format = Formatter([
            "Hold", "User", "Project", "Resource", "Datetime", "Held", "Comment"])
    else:
        format = Formatter([
            "Hold", "User", "Project", "Resource", "Datetime", "Held"])
    format.widths = {
        'Hold':6, 'User':8, 'Project':15, 'Held':13, 'Datetime':10}
    format.aligns = {'Held':"right"}
    print format.header()
    print format.bar()
    
    s = model.Session()
    now = datetime.now()
    
    query = sql.select([
        model.holds.c.id,
        model.holds.c.user_id,
        model.allocations.c.project_id,
        model.allocations.c.resource_id,
        model.holds.c.datetime,
        model.holds.c.amount,
        model.holds.c.comment]).where(model.holds.c.active==True)
    query = query.order_by(model.holds.c.datetime)
    query = query.select_from(
        model.holds.join(model.allocations))
    
    if kwargs.get("users"):
        query = query.where(model.holds.c.user_id.in_(
            user.id for user in kwargs.get("users")))
    if kwargs.get("projects"):
        query = query.where(model.allocations.c.project_id.in_(
            project.id for project in kwargs.get("projects")))
    if kwargs.get("resources"):
        query = query.where(model.allocations.c.resource_id.in_(
            resources.id for resources in kwargs.get("resources")))
    if kwargs.get("after"):
        query = query.where(
            model.holds.c.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        query = query.where(
            model.holds.c.datetime<kwargs.get("before"))
    
    total_held = 0
    for row in s.execute(query):
        hold = row[model.holds.c.id]
        comment = row[model.holds.c.comment]
        user = model.upstream.get_user_name(row[model.holds.c.user_id])
        project = model.project_by_id(
            row[model.allocations.c.project_id])
        resource = model.resource_by_id(
            row[model.allocations.c.resource_id])
        dt = row[model.holds.c.datetime]
        held = row[model.holds.c.amount]
        total_held += held
        if kwargs.get("comments"):
            print format({
                'Hold':hold,
                'User':user,
                'Project':project,
                'Resource':resource,
                'Datetime':format_datetime(dt),
                'Held':display_units(held),
                'Comment':comment})
        else:
            print format({
                'Hold':hold,
                'User':user,
                'Project':project,
                'Resource':resource,
                'Datetime':format_datetime(dt),
                'Held':display_units(held)})
    print format.bar(["Held"])
    print format({'Held':display_units(total_held)})
    print unit_definition()

def print_charges_report (**kwargs):
    
    """Charges report.
    
    The charges report displays individual charges.
    """
    
    if kwargs.get("comments"):
        format = Formatter(["Charge", "User", "Project", "Resource",
            "Datetime", "Charged", "Comment"])
    else:
        format = Formatter([
            "Charge", "User", "Project", "Resource", "Datetime", "Charged"])
    format.widths = {
        'Charge':6, 'User':8, 'Project':15, 'Charged':13, 'Datetime':10}
    format.aligns = {'Charged':"right"}
    print format.header()
    print format.bar()
    
    s = model.Session()
    now = datetime.now()
    
    query = sql.select([
        model.charges.c.id,
        model.charges.c.comment,
        model.charges.c.user_id,
        model.allocations.c.project_id,
        model.allocations.c.resource_id,
        model.charges.c.datetime,
        model.charges.c.amount])
    query = query.order_by(model.charges.c.datetime)
    query = query.select_from(
        model.charges.join(model.allocations))
    
    if kwargs.get("users"):
        query = query.where(model.charges.c.user_id.in_(
            user.id for user in kwargs.get("users")))
    if kwargs.get("projects"):
        query = query.where(model.allocations.c.project_id.in_(
            project.id for project in kwargs.get("projects")))
    if kwargs.get("resources"):
        query = query.where(model.allocations.c.resource_id.in_(
            resources.id for resources in kwargs.get("resources")))
    if kwargs.get("after"):
        query = query.where(
            model.charges.c.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        query = query.where(
            model.charges.c.datetime<kwargs.get("before"))
    
    total_charged = 0
    for row in s.execute(query):
        charge = row[model.charges.c.id]
        comment = row[model.charges.c.comment]
        user = model.upstream.get_user_name(row[model.charges.c.user_id])
        project = model.project_by_id(
            row[model.allocations.c.project_id])
        resource = model.resource_by_id(
            row[model.allocations.c.resource_id])
        dt = row[model.charges.c.datetime]
        charged = row[model.charges.c.amount]
        total_charged += charged
        if kwargs.get("comments"):
            print format({
                'Charge':charge,
                'User':user,
                'Project':project,
                'Resource':resource,
                'Datetime':format_datetime(dt),
                'Charged':display_units(charged),
                'Comment':comment})
        else:
            print format({
                'Charge':charge,
                'User':user,
                'Project':project,
                'Resource':resource,
                'Datetime':format_datetime(dt),
                'Charged':display_units(charged)})
    print format.bar(["Charged"])
    print format({'Charged':display_units(total_charged)})
    print unit_definition()

def print_allocations (allocations):
    for allocation in allocations:
        print_allocation(allocation)

def print_allocation (allocation):
    amount = display_units(allocation.amount)
    allocation_str = "Allocation %s -- %s" % (allocation, amount)
    print allocation_str
    print " * Datetime: %s" % allocation.datetime
    print " * Project: %s" % allocation.project
    print " * Resource: %s" % allocation.resource
    print " * Start: %s" % allocation.start
    print " * Expiration: %s" % allocation.expiration
    print " * Comment: %s" % allocation.comment

def print_holds (holds):
    for hold in holds:
        print_hold(hold)

def print_hold (hold):
    print "Hold %s -- %s" % (hold, display_units(hold.amount))
    print " * Datetime: %s" % hold.datetime
    print " * Active: %s" % hold.active
    print " * Allocation: %s" % hold.allocation
    print " * Project: %s" % hold.allocation.project
    print " * Resource: %s" % hold.allocation.resource
    print " * Comment: %s" % hold.comment

def print_charges (charges):
    for charge in charges:
        print_charge(charge)

def print_charge (charge):
    charge_str = "Charge %s -- %s" % (
        charge, display_units(charge.effective_amount))
    if charge.amount != charge.effective_amount:
        addendum = "(originally %s)" % display_units(amount)
        charge_str = " ".join([charge_str, addendum])
    print charge_str
    print " * Datetime: %s" % charge.datetime
    print " * Allocation: %s" % charge.allocation
    print " * Project: %s" % charge.allocation.project
    print " * Resource: %s" % charge.allocation.resource
    print " * Comment: %s" % charge.comment

def print_refunds (refunds):
    for refund in refunds:
        print_refund(refund)

def print_refund (refund):
    print "Refund %s -- %s" % (refund, display_units(refund.amount))
    print " * Datetime: %s" % refund.datetime
    print " * Charge: %s" % refund.charge
    print " * Allocation: %s" % refund.charge.allocation
    print " * Project: %s" % refund.charge.allocation.project
    print " * Resource: %s" % refund.charge.allocation.resource
    print " * Comment: %s" % refund.comment

def unit_definition ():
    try:
        unit_label = config.get("cbank", "unit_label")
    except ConfigParser.Error:
        return "Units are undefined."
    else:
        return "Units are in %s." % unit_label

def display_units (amount):
    converted_amount = convert_units(amount)
    if 0 < converted_amount < 0.1:
        return "< 0.1"
    else:
        return locale.format("%.1f", converted_amount, True)

def convert_units (amount):
    mul, div = get_unit_factor()
    return amount * mul / div

def format_datetime (dt):
    return dt.strftime("%Y-%m-%d")


class Formatter (object):
    
    def __init__ (self, fields, **kwargs):
        self.fields = fields
        self.headers = kwargs.get("headers", {})
        self.widths = kwargs.get("widths", {})
        self.aligns = kwargs.get("aligns", {})
    
    def __call__ (self, *args, **kwargs):
        return self.format(*args, **kwargs)
    
    def format (self, data):
        formatted_data = []
        for field in self.fields:
            datum = data.get(field, "")
            datum = str(datum)
            align = self._get_align(field)
            width = self._get_width(field)
            if align == "left":
                datum = datum.ljust(width)
            elif align == "right":
                datum = datum.rjust(width)
            elif align == "center":
                datum = datum.center(width)
            else:
                raise Exception("Unknown alignment: %s" % align)
            formatted_data.append(datum)
        return " ".join(formatted_data)
    
    def _get_align (self, field):
        return self.aligns.get(field, "left")
    
    def _get_width (self, field):
        return self.widths.get(field, len(self._get_header(field)))
    
    def _get_header (self, field):
        return self.headers.get(field, field)
    
    def bar (self, fields=None):
        if fields is None:
            fields = self.fields
        bars = {}
        for field in fields:
            bars[field] = "-" * (self._get_width(field))
        return self.format(bars)
    
    def header (self, fields=None):
        if fields is None:
            fields = self.fields
        headers = {}
        for field in fields:
            headers[field] = self._get_header(field)
        return self.format(headers)

