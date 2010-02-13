#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name="clusterbank",
    version="1.0",
    description="Accounting software for networked resources.",
    author="Jonathon Anderson",
    author_email="janderso@alcf.anl.gov",
    url="http://trac.mcs.anl.gov/projects/clusterbank",
    install_requires=["SQLAlchemy>=0.5.0"],
    packages=find_packages("source/packages", exclude=['ez_setup']),
    package_dir={'':"source/packages"},
    test_suite="nose.collector",
    zip_safe = True,
    entry_points = {'console_scripts': [
        "cbank = clusterbank.cbank.controllers:main",
        "cbank-new = clusterbank.cbank.controllers:new_main",
        "cbank-new-allocation = clusterbank.cbank.controllers:new_allocation_main",
        "cbank-new-hold = clusterbank.cbank.controllers:new_hold_main",
        "cbank-new-charge = clusterbank.cbank.controllers:new_charge_main",
        "cbank-new-refund = clusterbank.cbank.controllers:new_refund_main",
        "cbank-edit = clusterbank.cbank.controllers:edit_main",
        "cbank-edit-alloctaion = clusterbank.cbank.controllers:edit_allocation_main",
        "cbank-edit-hold = clusterbank.cbank.controllers:edit_hold_main",
        "cbank-edit-charge = clusterbank.cbank.controllers:edit_charge_main",
        "cbank-edit-refund = clusterbank.cbank.controllers:edit_refund_main",
        "cbank-import = clusterbank.cbank.controllers:import_main",
        "cbank-import-jobs = clusterbank.cbank.controllers:import_jobs_main",
        "cbank-list = clusterbank.cbank.controllers:list_main",
        "cbank-list-users = clusterbank.cbank.controllers:list_users_main",
        "cbank-list-projects = clusterbank.cbank.controllers:list_projects_main",
        "cbank-list-allocations = clusterbank.cbank.controllers:list_allocations_main",
        "cbank-list-holds = clusterbank.cbank.controllers:list_holds_main",
        "cbank-list-jobs = clusterbank.cbank.controllers:list_jobs_main",
        "cbank-list-charges = clusterbank.cbank.controllers:list_charges_main",
        "cbank-detail = clusterbank.cbank.controllers:detail_main",
        "cbank-detail-allocations = clusterbank.cbank.controllers:detail_allocations_main",
        "cbank-detail-holds = clusterbank.cbank.controllers:detail_holds_main",
        "cbank-detail-charges = clusterbank.cbank.controllers:detail_charges_main",
        "cbank-detail-refunds = clusterbank.cbank.controllers:detail_refunds_main"]},
    data_files=[("/etc", ["etc/clusterbank.conf"])])
