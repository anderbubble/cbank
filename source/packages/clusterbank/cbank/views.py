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

__all__ = ["unit_definition", "display_units",
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
    
    correlation = model.charges_table.c.user_id==model.users_table.c.id
    charges_count_query = sql.select([
        sql.func.count(model.charges_table.c.id)], correlation,
        from_obj=model.charges_table.join(model.allocations_table))
    charges_sum_query = sql.select([
        sql.func.sum(model.charges_table.c.amount, type_=IntSum)], correlation,
        from_obj=model.charges_table.join(model.allocations_table))
    refunds_sum_query = sql.select([
        sql.func.sum(model.refunds_table.c.amount, type_=IntSum)],
        correlation, from_obj=model.refunds_table.join(
        model.charges_table).join(model.allocations_table))
    
    condition = True
    if kwargs.get("projects"):
        condition = sql.and_(condition,
            model.allocations_table.c.project_id.in_(
                project.id for project in kwargs.get("projects")))
    if kwargs.get("resources"):
        condition = sql.and_(condition,
            model.allocations_table.c.resource_id.in_(
                resource.id for resource in kwargs.get("resources")))
    if kwargs.get("after"):
        condition = sql.and_(condition,
            model.charges_table.c.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        condition = sql.and_(condition,
            model.charges_table.c.datetime<kwargs.get("before"))
    charges_count_query = charges_count_query.where(condition)
    charges_sum_query = charges_sum_query.where(condition)
    refunds_sum_query = refunds_sum_query.where(condition)
    
    query = sql.select([
        model.users_table.c.id,
        charges_count_query.label("charges_count"),
        charges_sum_query.label("charges_sum"),
        refunds_sum_query.label("refunds_sum"),
        ]).select_from(model.users_table)
    
    if kwargs.get("users"):
        query = query.where(model.users_table.c.id.in_(
            user.id for user in kwargs.get("users")))
    
    total_charges = 0
    total_charged = 0
    for row in s.execute(query):
        user = model.upstream.get_user_name(row[model.users_table.c.id])
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
        model.allocations_table.c.project_id==model.projects_table.c.id
    allocations_sum_query = sql.select([
        sql.func.sum(model.allocations_table.c.amount, type_=IntSum)],
        correlation, from_obj=model.allocations_table)
    holds_sum_query = sql.select([
        sql.func.sum(model.holds_table.c.amount, type_=IntSum)],
        correlation, from_obj=model.holds_table.join(
            model.allocations_table)).where(model.holds_table.c.active==True)
    charges_sum_query = sql.select([
        sql.func.sum(model.charges_table.c.amount, type_=IntSum)],
        correlation, from_obj=model.charges_table.join(
            model.allocations_table))
    refunds_sum_query = sql.select([
        sql.func.sum(model.refunds_table.c.amount, type_=IntSum)],
        correlation, from_obj=model.refunds_table.join(
            model.charges_table).join(model.allocations_table))
    m_charges_count_query = sql.select([
        sql.func.count(model.charges_table.c.amount)], correlation,
            from_obj=model.charges_table.join(model.allocations_table))
    m_charges_sum_query = charges_sum_query
    m_refunds_sum_query = refunds_sum_query
    
    condition = sql.and_(model.allocations_table.c.start<=now,
            model.allocations_table.c.expiration>now)
    m_condition = True
    if kwargs.get("users"):
        m_condition = sql.and_(m_condition,
            model.charges_table.c.user_id.in_(
                user.id for user in kwargs.get("users")))
    if kwargs.get("resources"):
        _condition = model.allocations_table.c.resource_id.in_(
            resource.id for resource in kwargs.get("resources"))
        condition = sql.and_(condition, _condition)
        m_condition = sql.and_(m_condition, _condition)
    if kwargs.get("before"):
        m_condition = sql.and_(m_condition,
            model.charges_table.c.datetime<kwargs.get("before"))
    if kwargs.get("after"):
        m_condition = sql.and_(m_condition,
            model.charges_table.c.datetime>=kwargs.get("after"))
    allocations_sum_query = allocations_sum_query.where(condition)
    holds_sum_query = holds_sum_query.where(condition)
    charges_sum_query = charges_sum_query.where(condition)
    refunds_sum_query = refunds_sum_query.where(condition)
    m_charges_count_query = m_charges_count_query.where(m_condition)
    m_charges_sum_query = m_charges_sum_query.where(m_condition)
    m_refunds_sum_query = m_refunds_sum_query.where(m_condition)
    
    query = sql.select([
        model.projects_table.c.id,
        allocations_sum_query.label("allocations_sum"),
        holds_sum_query.label("holds_sum"),
        charges_sum_query.label("charges_sum"),
        refunds_sum_query.label("refunds_sum"),
        m_charges_count_query.label("m_charges_count"),
        m_charges_sum_query.label("m_charges_sum"),
        m_refunds_sum_query.label("m_refunds_sum")])
    
    query = query.select_from(model.projects_table)
    
    if kwargs.get("projects"):
        query = query.where(model.projects_table.c.id.in_(
            project.id for project in kwargs.get("projects")))
    
    total_available = 0
    total_charges = 0
    total_charged = 0
    for row in s.execute(query):
        project = \
            model.upstream.get_project_name(row[model.projects_table.c.id])
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
        model.charges_table.c.allocation_id==model.allocations_table.c.id
    charges_sum_query = sql.select([
        sql.func.sum(model.charges_table.c.amount, type_=IntSum)],
        correlation)
    refunds_sum_query = sql.select([
        sql.func.sum(model.refunds_table.c.amount, type_=IntSum)],
        correlation, from_obj=model.refunds_table.join(model.charges_table))
    holds_sum_query = sql.select([
        sql.func.sum(model.holds_table.c.amount, type_=IntSum)],
        correlation).where(model.holds_table.c.active==True)
    m_charges_count_query = sql.select([
        sql.func.count(model.charges_table.c.id)], correlation)
    m_charges_sum_query = charges_sum_query
    m_refunds_sum_query = refunds_sum_query
    
    m_condition = True
    if kwargs.get("users"):
        m_condition = sql.and_(m_condition, model.charges_table.c.user_id.in_(
            user.id for user in kwargs.get("users")))
    if kwargs.get("after"):
        m_condition = sql.and_(m_condition,
            model.charges_table.c.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        m_condition = sql.and_(m_condition,
            model.charges_table.c.datetime<kwargs.get("before"))
    m_charges_count_query = m_charges_count_query.where(m_condition)
    m_charges_sum_query = m_charges_sum_query.where(m_condition)
    m_refunds_sum_query = m_refunds_sum_query.where(m_condition)
    
    query = sql.select([
        model.allocations_table.c.id,
        model.projects_table.c.id,
        model.resources_table.c.id,
        model.allocations_table.c.expiration,
        model.allocations_table.c.amount,
        holds_sum_query.label("holds_sum"),
        charges_sum_query.label("charges_sum"),
        refunds_sum_query.label("refunds_sum"),
        m_charges_count_query.label("m_charges_count"),
        m_charges_sum_query.label("m_charges_sum"),
        m_refunds_sum_query.label("m_refunds_sum"),
        ], use_labels=True)
    
    query = query.select_from(model.allocations_table.join(
        model.projects_table).join(model.resources_table))
    
    if kwargs.get("projects"):
        query = query.where(model.projects_table.c.id.in_(
            project.id for project in kwargs.get("projects")))
    if kwargs.get("resources"):
        query = query.where(model.resources_table.c.id.in_(
            resource.id for resource in kwargs.get("resources")))
    if kwargs.get("after") or kwargs.get("before"):
        if kwargs.get("after"):
            query = query.where(
                model.allocations_table.c.expiration>kwargs.get("after"))
        if kwargs.get("before"):
            query = query.where(
                model.allocations_table.c.start<=kwargs.get("before"))
    else:
        query = query.where(sql.and_(model.allocations_table.c.start<=now,
            model.allocations_table.c.expiration>now))
    
    total_available = 0
    total_charges = 0
    total_charged = 0
    for row in s.execute(query):
        allocation = row[model.allocations_table.c.id]
        project = model.upstream.get_project_name(
            row[model.projects_table.c.id])
        resource = model.upstream.get_resource_name(
            row[model.resources_table.c.id])
        expiration = format_datetime(
            row[model.allocations_table.c.expiration])
        charges = row['m_charges_count']
        total_charges += charges
        available = row[model.allocations_table.c.amount] \
            - (row['holds_sum'] + (row['charges_sum'] - row['refunds_sum']))
        total_available += available
        charged = row['m_charges_sum'] - row['m_refunds_sum']
        total_charged += charged
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
        model.holds_table.c.id,
        model.holds_table.c.user_id,
        model.allocations_table.c.project_id,
        model.allocations_table.c.resource_id,
        model.holds_table.c.datetime,
        model.holds_table.c.amount]).where(model.holds_table.c.active==True)
    query = query.order_by(model.holds_table.c.datetime)
    query = query.select_from(
        model.holds_table.join(model.allocations_table))
    
    if kwargs.get("users"):
        query = query.where(model.holds_table.c.user_id.in_(
            user.id for user in kwargs.get("users")))
    if kwargs.get("projects"):
        query = query.where(model.allocations_table.c.project_id.in_(
            project.id for project in kwargs.get("projects")))
    if kwargs.get("resources"):
        query = query.where(model.allocations_table.c.resource_id.in_(
            resources.id for resources in kwargs.get("resources")))
    if kwargs.get("after"):
        query = query.where(
            model.holds_table.c.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        query = query.where(
            model.holds_table.c.datetime<kwargs.get("before"))
    
    total_held = 0
    for row in s.execute(query):
        hold = row[model.holds_table.c.id]
        user = model.upstream.get_user_name(row[model.holds_table.c.user_id])
        project = model.project_by_id(
            row[model.allocations_table.c.project_id])
        resource = model.resource_by_id(
            row[model.allocations_table.c.resource_id])
        dt = row[model.holds_table.c.datetime]
        held = row[model.holds_table.c.amount]
        total_held += held
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
        model.charges_table.c.id,
        model.charges_table.c.user_id,
        model.allocations_table.c.project_id,
        model.allocations_table.c.resource_id,
        model.charges_table.c.datetime,
        model.charges_table.c.amount])
    query = query.order_by(model.charges_table.c.datetime)
    query = query.select_from(
        model.charges_table.join(model.allocations_table))
    
    if kwargs.get("users"):
        query = query.where(model.charges_table.c.user_id.in_(
            user.id for user in kwargs.get("users")))
    if kwargs.get("projects"):
        query = query.where(model.allocations_table.c.project_id.in_(
            project.id for project in kwargs.get("projects")))
    if kwargs.get("resources"):
        query = query.where(model.allocations_table.c.resource_id.in_(
            resources.id for resources in kwargs.get("resources")))
    if kwargs.get("after"):
        query = query.where(
            model.charges_table.c.datetime>=kwargs.get("after"))
    if kwargs.get("before"):
        query = query.where(
            model.charges_table.c.datetime<kwargs.get("before"))
    
    total_charged = 0
    for row in s.execute(query):
        charge = row[model.charges_table.c.id]
        user = model.upstream.get_user_name(row[model.charges_table.c.user_id])
        project = model.project_by_id(
            row[model.allocations_table.c.project_id])
        resource = model.resource_by_id(
            row[model.allocations_table.c.resource_id])
        dt = row[model.charges_table.c.datetime]
        charged = row[model.charges_table.c.amount]
        total_charged += charged
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

def print_allocation (allocation):
    amount = display_units(allocation.amount)
    amount_available = display_units(allocation.amount_available)
    allocation_str = "Allocation #%i -- %s (%s available)" % (
        allocation.id, amount, amount_available)
    if allocation.comment:
        allocation_str = " ".join([allocation_str, "(%s)" % allocation.comment])
    print allocation_str
    print " * model.Project: '%s'" % allocation.project
    print " * model.Resource: '%s'" % allocation.resource
    print " * Start: %s" % allocation.start
    print " * Expiration: %s" % allocation.expiration

def print_charges (charges):
    for charge in charges:
        print_charge(charge)

def print_charge (charge):
    effective_amount = display_units(charge.effective_amount)
    amount = display_units(charge.amount)
    charge_str = "Charge #%i -- %s" % (charge.id, effective_amount)
    if amount != effective_amount:
        charge_str = " ".join([charge_str, "(originally %s)" % amount])
    if charge.comment:
        charge_str = " ".join([charge_str, "(%s)" % charge.comment])
    print charge_str
    print " * model.Allocation #%i -- %s (%s available)" % (
        charge.allocation.id, charge.allocation.amount,
        charge.allocation.amount_available)

def print_refund (refund):
    amount = display_units(refund.amount)
    refund_str = "Refund #%i -- %s" % (refund.id, amount)
    if refund.comment:
        refund_str = " ".join([refund_str, "(%s)" % refund.comment])
    print refund_str
    charge_effective_amount = display_units(refund.charge.effective_amount)
    charge_amount = display_units(refund.charge.amount)
    print " * model.Charge #%i -- %s (originally %s)" % (
        refund.charge.id, charge_effective_amount,
        charge_amount)

def unit_definition ():
    try:
        unit_label = config.get("cbank", "unit_label")
    except ConfigParser.Error:
        return "Units are undefined."
    else:
        return "Units are in %s." % unit_label

def display_units (amount):
    mul, div = get_unit_factor()
    converted_amount = amount * mul / div
    if 0 < converted_amount < 0.1:
        return "< 0.1"
    else:
        return locale.format("%.1f", converted_amount, True)

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

