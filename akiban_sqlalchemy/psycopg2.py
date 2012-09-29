"""Implement the Akiban dialect via an extended version of the psycopg2
driver.

"""
from __future__ import absolute_import

from .base import AkibanDialect, AkibanExecutionContext

Connection = None

class AkibanPsycopg2ExecutionContext(AkibanExecutionContext):
    def set_ddl_autocommit(self, connection, value):
        # this is psycopg2.autocommit:
        # http://initd.org/psycopg/docs/connection.html#connection.autocommit
        connection.commit()
        connection.autocommit = value

class AkibanPsycopg2Dialect(AkibanDialect):
    use_native_unicode = True
    execution_ctx_cls = AkibanPsycopg2ExecutionContext

    @classmethod
    def dbapi(cls):
        global Connection
        from akiban.psycopg2 import Connection
        return __import__("psycopg2")

    def on_connect(self):
        fns = []

        if self.dbapi and self.use_native_unicode:
            from psycopg2 import extensions
            def setup_unicode_extension(conn):
                extensions.register_type(extensions.UNICODE, conn)
            fns.append(setup_unicode_extension)

        if fns:
            def on_connect(conn):
                for fn in fns:
                    fn(conn)
            return on_connect
        else:
            return None

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'port' in opts:
            opts['port'] = int(opts['port'])
        opts.update(url.query)
        opts['connection_factory'] = Connection
        return ([], opts)
