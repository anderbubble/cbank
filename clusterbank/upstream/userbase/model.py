"""Userbase model for userbase plugin.

Classes:
User -- Upstream user.
Project -- Upstream project.
Resource -- Upstream resource.

Exception:
DoesNotExist -- The specified resource does not exist.
"""

import sqlalchemy as sa

import schema

session = sa.create_session()

class DoesNotExist (Exception):
    """The specified resource does not exist."""

class User (object):
    """Upstream user.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string identifier.
    uid -- Unix id.
    title -- Human title.
    firstname -- Human first name.
    middlename -- Human middle name.
    lastname -- Human last name.
    email -- Primary email address.
    activation_date -- Date the account was activated.
    deactivation_date -- Date the account was deactivated.
    projects -- Projects the user is a member of.
    
    Class methods:
    by_id -- Retrieve a user by identifier.
    by_name -- Retrieve a user by name.
    
    Exceptions:
    DoesNotExist -- The specified user does not exist.
    """
    
    class DoesNotExist (DoesNotExist):
        """The specified user does not exist."""
    
    @classmethod
    def by_id (cls, id):
        """Retrieve a user by identifier.
        
        Arguments:
        id -- Canonical, immutable, integer identifier.
        """
        user = session.query(cls).get_by(id=id)
        if not user:
            raise cls.DoesNotExist("There is no user %i." % id)
        return user
    
    @classmethod
    def by_name (cls, name):
        """Retrieve a user by name
        
        Arguments:
        name -- Canonical string identifier.
        """
        user = session.query(cls).get_by(name=name)
        if not user:
            raise cls.DoesNotExist("There is no user named '%s'." % name)
        return user
    
    def __repr__ (self):
        return "<%s %i>" % (self.__class__.__name__, self.id)
    
    def __str__ (self):
        return self.name


class Project (object):
    """Upstream project.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string id.
    description -- Verbose description.
    status -- Status of the project.
    users -- Users that are members of the project.
    resources -- Resources allocated to the project.
    
    Class methods:
    by_id -- Retrieve a project by identifier.
    by_name -- Retrieve a project by name.
    
    Exceptions:
    DoesNotExist -- The specified project does not exist.
    """
    
    class DoesNotExist (DoesNotExist):
        """The specified project does not exist."""
    
    @classmethod
    def by_id (cls, id):
        """Retrieve a project by identifier.
        
        Arguments:
        id -- Canonical, immutable, integer identifier.
        """
        project = session.query(cls).get_by(id=id)
        if not project:
            raise cls.DoesNotExist("There is no project %i." % id)
        return project
    
    @classmethod
    def by_name (cls, name):
        """Retrieve a project by name.
        
        Arguments:
        name -- Canonical string identifier.
        """
        project = session.query(cls).get_by(name=name)
        if not project:
            raise cls.DoesNotExist("There is no project named '%s'." % name)
        return project
    
    def __repr__ (self):
        return "<%s %i>" % (self.__class__.__name__, self.id)
    
    def __str__ (self):
        return self.name


class Resource (object):
    """Upstream resource.
    
    Attributes:
    id -- Canonical, immutable, integer identifier.
    name -- Canonical string identifier.
    projects -- Projects to which this resource has been allocated.
    
    Class methods:
    by_id -- Retrieve a resource by identifier.
    by_name -- Retrieve a resource by name.
    
    Exceptions:
    DoesNotExist -- The specified resource does not exist.
    """
    
    class DoesNotExist (DoesNotExist):
        """The specified resource does not exist."""
    
    @classmethod
    def by_id (cls, id):
        """Retrieve a resource by id.
        
        Arguments:
        id -- Canonical, immutable, integer identifier.
        """
        resource = session.query(cls).get_by(id=id)
        if not resource:
            raise cls.DoesNotExist("Could not find resource %i." % id)
        return resource
    
    @classmethod
    def by_name (cls, name):
        """Retrieve a resource by name.
        
        Arguments:
        name -- Canonical string identifier.
        """
        resource = session.query(cls).get_by(name=name)
        if not resource:
            raise cls.DoesNotExist("There is no resource named '%s'." % name)
        return resource
    
    def __repr__ (self):
        return "<%s %i>" % (self.__class__.__name__, self.id)
    
    def __str__ (self):
        return self.name

sa.mapper(User, schema.user_table, properties=dict(
    id = schema.user_table.c.userbase_id,
    name = schema.user_table.c.username,
    uid = schema.user_table.c.unix_id,
    title = schema.user_table.c.name_title,
    firstname = schema.user_table.c.name_first,
    middlename = schema.user_table.c.name_middle,
    lastname = schema.user_table.c.name_last,
    email = schema.user_table.c.preferred_email,
    activation_date = schema.user_table.c.activation_date,
    deactivation_date = schema.user_table.c.deactivation_date,
    projects = sa.relation(Project, secondary=schema.project_members_table)
))

sa.mapper(Project, schema.projects_table, properties=dict(
    id = schema.projects_table.c.project_id,
    name = schema.projects_table.c.project_name,
    description = schema.projects_table.c.description,
    status = schema.projects_table.c.status,
    users = sa.relation(User, secondary=schema.project_members_table),
    resources = sa.relation(Resource, secondary=schema.resources_to_projects_table)
))

sa.mapper(Resource, schema.resource_types_table, properties=dict(
    id = schema.resource_types_table.c.resource_id,
    name = schema.resource_types_table.c.resource_name,
    projects = sa.relation(Project, secondary=schema.resources_to_projects_table)
))
