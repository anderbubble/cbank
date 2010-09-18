cbank is an accounting system based on allocations, holds,
charges, and refunds.


Features
========

* plugin architecture for accessing existing directory services
* complete user- and admin-level command-line interface
* SQLAlchemy-based model supports many common databases


Command-line interface
======================

cbank's command-line interface is made up of a number of
subcommands.  For detailed help concerning a specific command,
use the -h (or --help) option.

Each of the commands are further divided into one of three main
categories: new, list, or edit.

New entities
------------

Create new entities.

* cbank new allocation
* cbank new hold
* cbank new charge
* cbank new refund

Edit entities
-------------

Modify entity attributes.

* cbank edit allocation
* cbank edit hold
* cbank edit charge
* cbank edit refund

List entities
-------------

Aggregate data from a number of perspectives.

* cbank list users
* cbank list projects
* cbank list allocations
* cbank list holds
* cbank list charges

Detail entities
---------------

Full detail on specific entities.

* cbank detail allocations
* cbank detail holds
* cbank detail charges
* cbank detail refunds


Upstream plugins
================

cbank uses a plugin system to access certain upstream entities:

* users
* projects
* resources

The included upstream module, cbank.upstreams.posix, derives users,
projects, and resources from the local nameservice.  Users are derived
from the pwd module.  Projects and resources are derived from the grp
module.

The upstream module is specified using the module configuration
variable in the [upstream] section.

An upstream plugin is a python module with a series of defined
functions:

Input functions
---------------

These functions transform a user-provided representation of an
upstream entity into a unique id to be used in the database.

* user_in
* project_in
* resource_in

Output functions
----------------

These functions transform an entity's internal id into a
user-visible representation.

* user_out
* project_out
* resource_out

Membership functions
--------------------

These functions depict the relationship between projects
and users.

* project_member
* project_manager
