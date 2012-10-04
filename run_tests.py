from sqlalchemy.dialects import registry

registry.register("akiban", "sqlalchemy_akiban.dialect.psycopg2", "AkibanPsycopg2Dialect")
registry.register("akiban.psycopg2", "sqlalchemy_akiban.dialect.psycopg2", "AkibanPsycopg2Dialect")

from sqlalchemy.testing import runner

runner.main()
