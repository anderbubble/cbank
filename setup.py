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
    version="trunk",
    description="Accounting software for networked resources.",
    author="Jonathon Anderson",
    author_email="janderso@alcf.anl.gov",
    url="http://trac.mcs.anl.gov/projects/clusterbank",
    install_requires=["SQLAlchemy >=0.4.4, <=0.4.6"],
    packages=find_packages("source/packages", exclude=['ez_setup']),
    package_dir={'':"source/packages"},
    test_suite="nose.collector",
    zip_safe = True,
    entry_points = {'console_scripts': [
            "cbank = clusterbank.cbank.controllers:main",
            "cbank-report = clusterbank.cbank.controllers:report_main",
            "cbank-report-users = clusterbank.cbank.controllers:report_users_main",
            "cbank-report-projects = clusterbank.cbank.controllers:report_projects_main",
            "cbank-report-allocations = clusterbank.cbank.controllers:report_allocations_main",
            "cbank-report-charges = clusterbank.cbank.controllers:report_charges_main",
            "cbank-new = clusterbank.cbank.controllers:new_main",
            "cbank-new-allocation = clusterbank.cbank.controllers:new_allocation_main",
            "cbank-new-charge = clusterbank.cbank.controllers:new_charge_main",
            "cbank-new-refund = clusterbank.cbank.controllers:new_refund_main",
    ]},
)
