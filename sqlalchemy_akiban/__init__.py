__version__ = '0.5'

from sqlalchemy.dialects import registry

registry.register("akiban", "sqlalchemy_akiban.psycopg2", "AkibanPsycopg2Dialect")
registry.register("akiban+psycopg2", "sqlalchemy_akiban.psycopg2", "AkibanPsycopg2Dialect")

