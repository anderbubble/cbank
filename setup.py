#!/usr/bin/env python

import os

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name="clusterbank",
    version = "0.2.x",
    description = "Accounting software for networked resources.",
    author="Jonathon Anderson",
    author_email="janderso@alcf.anl.gov",
    url = "http://trac.mcs.anl.gov/projects/clusterbank",
    install_requires=["SQLAlchemy>=0.4.0"],
    packages=find_packages("source/packages", exclude=['ez_setup']),
    package_dir = {"":"source/packages"},
    test_suite="nose.collector",
    zip_safe = True,
    scripts = [os.path.join("source", "scripts", script)
               for script in ["cb-install",
                              "cb-request", "cb-allocate", "cb-hold",
                              "cb-charge", "cb-refund"]],
)
