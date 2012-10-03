from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions

class Requirements(SuiteRequirements):
    @property
    def foreign_key_ddl(self):
        return exclusions.open()

    @property
    def self_referential_foreign_keys(self):
        return exclusions.closed()

    @property
    def table_reflection(self):
        return exclusions.closed()

    @property
    def view_reflection(self):
        return exclusions.closed()

    @property
    def schema_reflection(self):
        return exclusions.closed()

    @property
    def primary_key_constraint_reflection(self):
        return exclusions.closed()

    @property
    def foreign_key_constraint_reflection(self):
        return exclusions.closed()

    @property
    def index_reflection(self):
        return exclusions.closed()

    @property
    def returning(self):
        # not seeing RETURNING working yet
        return exclusions.closed()