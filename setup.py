#!/usr/bin/env python

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name="cbank",
    version="1.2.2",
    description="Abstract accounting system",
    author="Jonathon Anderson",
    author_email="anderbubble@gmail.com",
    url="http://www.civilfritz.net/wiki/projects/cbank",
    install_requires=["SQLAlchemy>=0.5", "decorator"],
    packages=find_packages("source/packages", exclude=['ez_setup']),
    package_dir={'':"source/packages"},
    test_suite="nose.collector",
    tests_require=["nose", "mock", "pysqlite"],
    zip_safe = True,
    entry_points = {'console_scripts': [
        "cbank = cbank.cli.controllers:main"]},
    data_files=[("etc", ["etc/cbank.conf"])])
