#!/usr/bin/env python

import os
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name = "clusterbank",
    version = "0.2.x",
    description = "Accounting software for networked resources.",
    author = "Jonathon Anderson",
    author_email = "janderso@mcs.anl.gov",
    url = "http://trac.mcs.anl.gov/projects/clusterbank",
    package_dir = {'': os.path.join("source", "packages")},
    packages = ["clusterbank", "clusterbank.upstream",
                "clusterbank.upstream.userbase"],
    zip_safe = True,
    scripts = [os.path.join("source", "scripts", script)
               for script in ["cb-install", "cb-admin", "cb-request",
                              "cb-allocate", "cb-lien", "cb-refund",
                              "cb-request"]],
    provides = ["clusterbank"],
    requires = ["sqlalchemy"],
    install_requires = ["SQLAlchemy>=0.4"],
)
