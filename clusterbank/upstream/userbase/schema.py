"""Schema for userbase database.

Tables:
user_table -- Upstream users.
projects_table -- Upstream projects.
project_members_table -- Upstream project membership.
resource_types_table -- Upstream resources.
resources_to_projects_table -- Upstream project resource allocation.

Objects:
metadata -- Binding between tables.
"""

import sqlalchemy as sa

metadata = sa.MetaData(name="userbase")

user_table = sa.Table("user", metadata,
    sa.Column("username", sa.String(16), nullable=False, unique=True),
    sa.Column("unix_id", sa.Integer, nullable=True, default=0),
    sa.Column("account_type_id", sa.Integer, nullable=True, default=None),
    sa.Column("status", sa.String(16), nullable=True, default=None),
    sa.Column("activation_date", sa.Date, nullable=False),
    sa.Column("deactivation_date", sa.Date, nullable=True, default=None),
    sa.Column(
        "encrypted_default_password", sa.String(13),
        nullable=True, default=None),
    sa.Column("policy_agreed", sa.String(1), nullable=True, default="N"),
    sa.Column("name_title", sa.String(8), nullable=True, default=None),
    sa.Column("name_first", sa.String(25), nullable=True, default=None),
    sa.Column("name_last", sa.String(40), nullable=True, default=None),
    sa.Column("office", sa.String(30), nullable=True, default=None),
    sa.Column("office_phone", sa.String(30), nullable=True, default=None),
    sa.Column("home_line_1", sa.String(40), nullable=True, default=None),
    sa.Column("home_line_2", sa.String(40), nullable=True, default=None),
    sa.Column("home_city", sa.String(32), nullable=True, default=None),
    sa.Column("home_state", sa.String(2), nullable=True, default=None),
    sa.Column("home_zip", sa.String(12), nullable=True, default=None),
    sa.Column("home_country", sa.String(40), nullable=True, default=None),
    sa.Column("home_phone", sa.String(40), nullable=True, default=None),
    sa.Column("alt_email", sa.String(64), nullable=True, default=None),
    sa.Column("comment", sa.String(128), nullable=True, default=None),
    sa.Column("us_citizen", sa.String(1), nullable=True, default="N"),
    sa.Column("userbase_id", sa.Integer, nullable=False, primary_key=True),
    sa.Column("name_middle", sa.String(25), nullable=True, default=None),
    sa.Column("affiliation", sa.String(100), nullable=True, default=None),
    sa.Column("shell", sa.String(50), nullable=True, default=None),
    sa.Column("restricted", sa.String(1), nullable=True, default="N"),
    sa.Column("cracked", sa.String(1), nullable=True, default="N"),
    sa.Column("duration", sa.String, nullable=True, default=None),
    sa.Column("project_desc", sa.String, nullable=True, default=None),
    sa.Column("random_password", sa.String(20), nullable=True, default=None),
    sa.Column(
        "account_administrator", sa.String(1),
        nullable=True, default=None),
    sa.Column("account_approver", sa.String(1), nullable=True, default=None),
    sa.Column("admin", sa.String(1), nullable=True, default=None),
    sa.Column(
        "notification_style", sa.String(6),
        nullable=True, default="once"),
    sa.Column("notify_next", sa.String(1), nullable=True, default="Y"),
    sa.Column("home_directory", sa.String(50), nullable=True, default=None),
    sa.Column("preferred_name", sa.String(80), nullable=True, default=None),
    sa.Column("validation_date", sa.Date, nullable=True, default=None),
    sa.Column("refresh_user", sa.String(1), nullable=True, default="Y"),
    sa.Column("deact_warning_level", sa.Integer, nullable=True, default=0),
    sa.Column("forward_email", sa.String(1), nullable=True, default="N"),
    sa.Column("creation_date", sa.Date, nullable=True, default=None),
    sa.Column("badge_number", sa.Integer, nullable=True, default=None),
    sa.Column("lcrc_notify_next", sa.String(1), nullable=True, default="Y"),
    sa.Column("security_answer", sa.String, nullable=True, default=None),
    sa.Column("security_question", sa.String, nullable=True, default=None),
    sa.Column("public_key", sa.String, nullable=True, default=None),
    sa.Column("secretary_id", sa.Integer, nullable=True, default=None),
    sa.Column("can_sponsor", sa.String(1), nullable=True, default="N"),
    sa.Column("mail_line_1", sa.String(40), nullable=True, default=None),
    sa.Column("mail_line_2", sa.String(40), nullable=True, default=None),
    sa.Column("mail_city", sa.String(32), nullable=True, default=None),
    sa.Column("mail_state", sa.String(2), nullable=True, default=None),
    sa.Column("mail_zip", sa.String(12), nullable=True, default=None),
    sa.Column("mail_country", sa.String(40), nullable=True, default=None),
    sa.Column("preferred_email", sa.String(64), nullable=True, default=None),
    sa.Column("ci_unix_id", sa.Integer, nullable=True, default=None)
)

projects_table = sa.Table("projects", metadata,
    sa.Column("project_id", sa.Integer, primary_key=True),
    sa.Column("project_name", sa.String(60), nullable=False, unique=True),
    sa.Column("description", sa.String, nullable=True, default=None),
    sa.Column("title", sa.String, nullable=True, default=None),
    sa.Column("status", sa.String(10), nullable=False, default="requested"),
    sa.Column("url", sa.String(255), nullable=True, default=None),
    sa.Column("institution", sa.String(128), nullable=True, default=None),
    sa.Column("division", sa.String(128), nullable=True, default=None),
    sa.Column("field", sa.String(128), nullable=True, default=None),
    sa.Column("funding_agency", sa.String(255), nullable=True, default=None),
    sa.Column("other_systems", sa.String, nullable=True, default=None),
    sa.Column("science", sa.String, nullable=True, default=None),
    sa.Column("priority", sa.Integer, nullable=True, default=None),
    sa.Column("point_of_contact", sa.String(128), nullable=True, default=None),
    sa.Column("poc_email", sa.String(128), nullable=True, default=None),
    sa.Column("associated_group", sa.String(128), nullable=True, default=None),
    sa.Column("csac_new", sa.String(1), nullable=False, default="N"),
    sa.Column("fy_requested", sa.Integer, nullable=True, default=None),
    sa.Column("justification", sa.String, nullable=True, default=None)
)

project_members_table = sa.Table("project_members", metadata,
    sa.Column(
        "userbase_id", sa.Integer, sa.ForeignKey("user.userbase_id"),
        primary_key=True),
    sa.Column(
        "project_id", sa.Integer, sa.ForeignKey("projects.project_id"),
        primary_key=True),
    sa.Column("default_project", sa.String(1), nullable=True, default="Y"),
    sa.Column(
        "principal_investigator", sa.String(1),
        nullable=True, default="N")
)

resource_types_table = sa.Table("resource_types", metadata,
    sa.Column("pretty_name", sa.String(32), nullable=False, unique=True),
    sa.Column("resource_name", sa.String(32), nullable=False, unique=True),
    sa.Column("resource_id", sa.Integer, nullable=False, primary_key=True),
    sa.Column("script_host", sa.String(48), nullable=True, default=None),
    sa.Column(
        "create_script_name", sa.String(128),
        nullable=True, default=None),
    sa.Column("authinfo", sa.String(32), nullable=True, default=None),
    sa.Column("resource_text", sa.String(255), nullable=True, default=None),
    sa.Column("automatic", sa.String(1), nullable=True, default=None),
    sa.Column("approve", sa.String(1), nullable=True, default=None),
    sa.Column("selectable", sa.String(1), nullable=True, default=None),
    sa.Column("display_order", sa.Integer, nullable=True, default=None),
    sa.Column("script_method", sa.String(10), nullable=True, default=None),
    sa.Column("script_user", sa.String(10), nullable=True, default=None),
    sa.Column("resource_info", sa.String, nullable=True, default=None),
    sa.Column("passwd_entry", sa.String(1), nullable=True, default=None),
    sa.Column(
        "deact_script_name", sa.String(128),
        nullable=True, default=None),
    sa.Column(
        "delete_script_name", sa.String(128),
        nullable=True, default=None),
    sa.Column("create_arguments", sa.String(128), nullable=True, default=None),
    sa.Column("deact_arguments", sa.String(128), nullable=True, default=None),
    sa.Column("delete_arguments", sa.String(128), nullable=True, default=None),
    sa.Column("obsolete", sa.String(1), nullable=True, default="N"),
    sa.Column("currently_down", sa.String(1), nullable=True, default="N"),
    # 'class' is a Python reserved word.
    # sa.Column("class", sa.String(128), nullable=True, default=None),
    sa.Column(
        "copies_existing_passwd", sa.String(1),
        nullable=False, default="N"),
    sa.Column(
        "generate_initial_passwd", sa.String(1),
        nullable=False, default="N")
)

resources_to_projects_table = sa.Table("resources_to_projects", metadata,
    sa.Column(
        "resource_id", sa.Integer,
        sa.ForeignKey("resource_types.resource_id"), primary_key=True,
        nullable=False, default=0),
    sa.Column(
        "project_id", sa.Integer,
        sa.ForeignKey("projects.project_id"), primary_key=True,
        nullable=False, default=0),
    sa.Column("selectable", sa.String(1), nullable=True, default="Y"),
    sa.Column("allocation", sa.Integer, nullable=True, default=0),
    sa.Column("requested", sa.Integer, nullable=True, default=0),
    sa.Column("addition_reason", sa.String, nullable=True, default=None)
)
