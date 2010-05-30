from nose.tools import raises, assert_equal

import clusterbank.upstreams.default
from clusterbank.upstreams.default import (
    user_in, user_out,
    project_in, project_out,
    resource_in, resource_out,
    project_member, project_manager,
    User, Project, Resource)


class UpstreamTester (object):

    def teardown (self):
        clusterbank.upstreams.default.users = []
        clusterbank.upstreams.default.projects = []
        clusterbank.upstreams.default.resources = []


class TestProject (UpstreamTester):

    def setup (self):
        project = Project("1", "Shrubbery")
        project.members = [User("1", "Monty")]
        project.managers = [User("2", "Python")]
        clusterbank.upstreams.default.projects = [project]
        clusterbank.upstreams.default.users = project.members + project.managers

    def test_out (self):
        assert_equal(project_out("1"), "Shrubbery")
        assert_equal(project_out("2"), None)

    def test_id (self):
        assert_equal(project_in("Spam"), None)
        assert_equal(project_in("Shrubbery"), "1")

    def test_member (self):
        assert not project_member("2", "1")
        assert project_member("1", "1")

    def test_manager (self):
        assert not project_manager("2", "1")
        assert project_manager("1", "2")


class TestResource (UpstreamTester):

    def setup (self):
        clusterbank.upstreams.default.resources = [
            Resource("1", "Spam"),
            Resource("3", "Life")]

    def test_out (self):
        assert_equal(resource_out("1"), "Spam")
        assert_equal(resource_out("2"), None)
        assert_equal(resource_out("3"), "Life")

    def test_in (self):
        assert_equal(resource_in("Spam"), "1")
        assert_equal(resource_in("more spam"), None)
        assert_equal(resource_in("Life"), "3")


class TestUser (UpstreamTester):

    def setup (self):
        user = User("1", "Monty")
        shrubbery = Project("1", "Shrubbery")
        spam = Project("2", "Spam")
        shrubbery.members = [user]
        spam.managers = [user]
        clusterbank.upstreams.default.projects = [shrubbery, spam]
        clusterbank.upstreams.default.users = [user]

    def test_in (self):
        assert_equal(user_in("Monty"), "1")
        assert_equal(user_in("Python"), None)

    def test_out (self):
        assert_equal(user_out("2"), None)
        assert_equal(user_out("1"), "Monty")
