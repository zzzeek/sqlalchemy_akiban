from sqlalchemy.dialects import registry

registry.register("akiban", "akiban_sqlalchemy.psycopg2", "AkibanPsycopg2Dialect")
registry.register("akiban+psycopg2", "akiban_sqlalchemy.psycopg2", "AkibanPsycopg2Dialect")

