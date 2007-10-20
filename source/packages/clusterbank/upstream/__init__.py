"""Plugins for user/project/resource tracking are defined here.

Plugins should make the plugin interface available at the top level (whether
that be a module or package).

class User:
    
    id = int
    name = str
    projects = iterable <Project>
    
    @classmethod
    def by_id (cls, id): --> Resource | DoesNotExist
    
    @classmethod
    def by_name (cls, name): --> Resource | DoesNotExist


class Project:
    
    id = int
    name = str
    users = iterable <User>
    
    @classmethod
    def by_id (cls, id): --> Resource | DoesNotExist
    
    @classmethod
    def by_name (cls, name): --> Resource | DoesNotExist


class Resource:
    
    id = int
    name = str
    
    @classmethod
    def by_id (cls, id): --> Resource | DoesNotExist
    
    @classmethod
    def by_name (cls, name): --> Resource | DoesNotExist

class DoesNotExist (Exception)
"""

__all__ = ["userbase"]
