
import re

from sqlalchemy import sql, exc, util
from sqlalchemy.engine import default, reflection, ResultProxy
from sqlalchemy.sql import compiler, expression
from sqlalchemy import types as sqltypes
from akiban.api import NESTED_CURSOR
from sqlalchemy.ext.compiler import compiles
import collections

from sqlalchemy.types import INTEGER, BIGINT, SMALLINT, VARCHAR, \
        CHAR, TEXT, FLOAT, NUMERIC, \
        DATE, BOOLEAN, REAL, TIMESTAMP, \
        TIME

RESERVED_WORDS = set(
    ["all", "analyse", "analyze", "and", "any", "array", "as", "asc",
    "asymmetric", "both", "case", "cast", "check", "collate", "column",
    "constraint", "create", "current_catalog", "current_date",
    "current_role", "current_time", "current_timestamp", "current_user",
    "default", "deferrable", "desc", "distinct", "do", "else", "end",
    "except", "false", "fetch", "for", "foreign", "from", "grant", "group",
    "having", "in", "initially", "intersect", "into", "leading", "limit",
    "localtime", "localtimestamp", "new", "not", "null", "off", "offset",
    "old", "on", "only", "or", "order", "placing", "primary", "references",
    "returning", "select", "session_user", "some", "symmetric", "table",
    "then", "to", "trailing", "true", "union", "unique", "user", "using",
    "variadic", "when", "where", "window", "with", "authorization",
    "between", "binary", "cross", "current_schema", "freeze", "full",
    "ilike", "inner", "is", "isnull", "join", "left", "like", "natural",
    "notnull", "outer", "over", "overlaps", "right", "similar", "verbose"
    ])

_DECIMAL_TYPES = (1231, 1700)
_FLOAT_TYPES = (700, 701, 1021, 1022)
_INT_TYPES = (20, 21, 23, 26, 1005, 1007, 1016)


class NestedResult(sqltypes.TypeEngine):

    def akiban_result_processor(self, gen_nested_context):
        def process(value):
            return ResultProxy(gen_nested_context(value))
        return process

class nested(expression.ScalarSelect):
    __visit_name__ = 'akiban_nested'

    def __init__(self, stmt):
        if isinstance(stmt, expression.ScalarSelect):
            stmt = stmt.element
        elif not isinstance(stmt, expression.SelectBase):
            stmt = expression.select(util.to_list(stmt))

        super(nested, self).__init__(stmt)
        self.type = NestedResult()



colspecs = {
}

ischema_names = {
    'integer': INTEGER,
    'bigint': BIGINT,
    'smallint': SMALLINT,
    'character varying': VARCHAR,
    'character': CHAR,
    '"char"': sqltypes.String,
    'name': sqltypes.String,
    'text': TEXT,
    'numeric': NUMERIC,
    'float': FLOAT,
    'real': REAL,
    'timestamp': TIMESTAMP,
    'timestamp with time zone': TIMESTAMP,
    'timestamp without time zone': TIMESTAMP,
    'time with time zone': TIME,
    'time without time zone': TIME,
    'date': DATE,
    'time': TIME,
    'boolean': BOOLEAN,
}

@compiles(nested)
def _visit_akiban_nested(nested, compiler, **kw):
    saved_result_map = compiler.result_map
    if hasattr(compiler, '_akiban_nested'):
        compiler.result_map = compiler._akiban_nested[nested.type] = {}
    try:
        kw['force_result_map'] = True
        return compiler.visit_grouping(nested, **kw)
    finally:
        compiler.result_map = saved_result_map

class AkibanCompiler(compiler.SQLCompiler):

    @util.memoized_property
    def _akiban_nested(self):
        return {}

    def render_literal_value(self, value, type_):
        value = super(AkibanCompiler, self).render_literal_value(value, type_)
        # TODO: need to inspect "standard_conforming_strings"
        if self.dialect._backslash_escapes:
            value = value.replace('\\', '\\\\')
        return value

    def limit_clause(self, select):
        text = ""
        if select._limit is not None:
            text += " \n LIMIT " + self.process(sql.literal(select._limit))
        if select._offset is not None:
            text += " OFFSET " + self.process(sql.literal(select._offset))
            if select._limit is None:
                text += " ROWS"  # OFFSET n ROW[S]
        return text

    def returning_clause(self, stmt, returning_cols):
        columns = [
                self._label_select_column(None, c, True, False,
                                    dict(include_table=False))
                for c in expression._select_iterables(returning_cols)
            ]

        return 'RETURNING ' + ', '.join(columns)


class AkibanDDLCompiler(compiler.DDLCompiler):
    def get_column_specification(self, column, **kwargs):

        colspec = self.preparer.format_column(column)
        colspec += " " + self.dialect.type_compiler.process(column.type)

        if column.nullable is not None:
            if not column.nullable:
                colspec += " NOT NULL"
            else:
                colspec += " NULL"

        if column is column.table._autoincrement_column:
            colspec += " GENERATED BY DEFAULT AS IDENTITY"
            # TODO: can do start with/increment by here
            # seq_col = column.table._autoincrement_column
        else:
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        return colspec


    def visit_foreign_key_constraint(self, constraint):
        preparer = self.dialect.identifier_preparer
        text = ""
        if constraint.name is not None:
            text += "CONSTRAINT %s " % \
                        preparer.format_constraint(constraint)
        remote_table = list(constraint._elements.values())[0].column.table
        text += "GROUPING FOREIGN KEY(%s) REFERENCES %s (%s)" % (
            ', '.join(preparer.quote(f.parent.name, f.parent.quote)
                      for f in constraint._elements.values()),
            self.define_constraint_remote_table(
                            constraint, remote_table, preparer),
            ', '.join(preparer.quote(f.column.name, f.column.quote)
                      for f in constraint._elements.values())
        )
        #text += self.define_constraint_match(constraint)
        #text += self.define_constraint_cascades(constraint)
        #text += self.define_constraint_deferrability(constraint)
        return text



class AkibanTypeCompiler(compiler.GenericTypeCompiler):
    pass

class AkibanIdentifierPreparer(compiler.IdentifierPreparer):

    reserved_words = RESERVED_WORDS

class AkibanInspector(reflection.Inspector):
    pass



class AkibanExecutionContext(default.DefaultExecutionContext):
    def get_result_processor(self, type_, colname, coltype):
        if self.compiled and type_ in self.compiled._akiban_nested:
            class NestedContext(object):
                result_map = self.compiled._akiban_nested[type_]
                dialect = self.dialect
                root_connection = self.root_connection
                engine = self.engine
                _translate_colname = None
                get_result_processor = self.get_result_processor

                def __init__(self, value):
                    self.cursor = value

            return type_.akiban_result_processor(NestedContext)
        else:
            return type_._cached_result_processor(self.dialect, coltype)

    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
                "select nextval('%s', '%s')" % (
                    seq.schema or self.dialect.default_schema_name,
                    self.dialect.identifier_preparer.format_sequence(seq)),
                type_)

    def set_ddl_autocommit(self, connection, value):
        """Must be implemented by subclasses to accommodate DDL executions.

        "connection" is the raw unwrapped DBAPI connection.   "value"
        is True or False.  when True, the connection should be configured
        such that a DDL can take place subsequently.  when False,
        a DDL has taken place and the connection should be resumed
        into non-autocommit mode.

        """
        raise NotImplementedError()

    def _table_identity_sequence(self, table):
        if '_akiban_identity_sequence' not in table.info:
            schema = table.schema or self.dialect.default_schema_name
            conn = self.root_connection
            conn._cursor_execute(
                self.cursor,
                "select sequence_name from "
                "information_schema.columns "
                "where schema_name=%(schema)s and "
                "table_name=%(tname)s",
                {
                    "tname": table.name,
                    "schema": schema
                }
            )
            table.info['_akiban_identity_sequence'] = \
                (schema, self.cursor.fetchone()[0])
        return table.info['_akiban_identity_sequence']

    def pre_exec(self):
        if self.isddl:
            # TODO: to enhance this, we can detect "ddl in tran" on the
            # database settings.  this error message should be improved to
            # include a note about that.
            if not self.should_autocommit:
                raise exc.InvalidRequestError(
                        "The Akiban dialect only supports "
                        "DDL in 'autocommit' mode at this time.")

            self.root_connection.engine.logger.info(
                        "AUTOCOMMIT (Assuming no Akiban 'ddl in tran')")

            self.set_ddl_autocommit(
                        self.root_connection.connection.connection,
                        True)

    def post_exec(self):
        if self.isddl:
            self.set_ddl_autocommit(
                        self.root_connection.connection.connection,
                        False)

    def get_lastrowid(self):
        assert self.isinsert, "lastrowid only supported with "\
                    "compiled insert() construct."
        tbl = self.compiled.statement.table

        seq_column = tbl._autoincrement_column
        insert_has_sequence = seq_column is not None
        if insert_has_sequence:
            schema_, sequence_name = self._table_identity_sequence(tbl)
            self.root_connection._cursor_execute(self.cursor,
                    "SELECT currval(%s, %s) AS lastrowid",
                        (schema_, sequence_name),
                    self)
            return self.cursor.fetchone()[0]
        else:
            return None


class AkibanDialect(default.DefaultDialect):
    name = 'akiban'
    supports_alter = False
    max_identifier_length = 63
    supports_sane_rowcount = True

    supports_native_enum = False
    supports_native_boolean = False

    supports_sequences = False  # TODO: True
    sequences_optional = True
    preexecute_autoincrement_sequences = False
    postfetch_lastrowid = True

    supports_default_values = True
    supports_empty_insert = False
    default_paramstyle = 'pyformat'
    ischema_names = ischema_names
    colspecs = colspecs

    statement_compiler = AkibanCompiler
    ddl_compiler = AkibanDDLCompiler
    type_compiler = AkibanTypeCompiler
    preparer = AkibanIdentifierPreparer
    execution_ctx_cls = AkibanExecutionContext
    inspector = AkibanInspector
    isolation_level = None

    dbapi_type_map = {
        NESTED_CURSOR: NestedResult()
    }
    # TODO: need to inspect "standard_conforming_strings"
    _backslash_escapes = True

    def __init__(self, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)

    def initialize(self, connection):
        super(AkibanDialect, self).initialize(connection)

    def on_connect(self):
        return None

    def _get_default_schema_name(self, connection):
        return connection.scalar("select CURRENT_USER")

    def has_schema(self, connection, schema):
        raise NotImplementedError("has_schema")

    def has_table(self, connection, table_name, schema=None):
        # seems like case gets folded in pg_class...
        if schema is not None:
            raise NotImplementedError("remote schemas")
        else:
            schema = self.default_schema_name

        cursor = connection.execute(
            sql.text(
            "select table_name from information_schema.tables "
            "where table_schema=:schema and table_name=:tname"
            ),
            {"tname": table_name, "schema": schema}
        )
        return bool(cursor.first())

    def has_sequence(self, connection, sequence_name, schema=None):
        raise NotImplementedError("has sequence")

    def _get_server_version_info(self, connection):
        ver = connection.scalar("select server_version from "
                    "information_schema.server_instance_summary")
        return ver

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        raise NotImplementedError("schema names")

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        if schema is not None:
            raise NotImplementedError("remote schemas")
        else:
            schema = self.default_schema_name

        cursor = connection.execute(
            sql.text(
            "select table_name from information_schema.tables "
            "where table_schema=:schema"
            ),
            {"schema": schema}
        )
        return [row[0] for row in cursor.fetchall()]


    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        raise NotImplementedError("view names")

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        raise NotImplementedError("view definition")

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        raise NotImplementedError()

    def _get_column_info(self, name, format_type, default,
                         notnull, schema):
        ## strip (*) from character varying(5), timestamp(5)
        # with time zone, geometry(POLYGON), etc.
        attype = re.sub(r'\(.*\)', '', format_type)

        # strip '[]' from integer[], etc.
        attype = re.sub(r'\[\]', '', attype)

        nullable = not notnull
        charlen = re.search('\(([\d,]+)\)', format_type)
        if charlen:
            charlen = charlen.group(1)
        args = re.search('\((.*)\)', format_type)
        if args and args.group(1):
            args = tuple(re.split('\s*,\s*', args.group(1)))
        else:
            args = ()
        kwargs = {}

        if attype == 'numeric':
            if charlen:
                prec, scale = charlen.split(',')
                args = (int(prec), int(scale))
            else:
                args = ()
        elif attype == 'double precision':
            args = (53, )
        elif attype == 'integer':
            args = ()
        elif attype in ('timestamp with time zone',
                        'time with time zone'):
            kwargs['timezone'] = True
            if charlen:
                kwargs['precision'] = int(charlen)
            args = ()
        elif attype in ('timestamp without time zone',
                        'time without time zone', 'time'):
            kwargs['timezone'] = False
            if charlen:
                kwargs['precision'] = int(charlen)
            args = ()
        elif attype == 'bit varying':
            kwargs['varying'] = True
            if charlen:
                args = (int(charlen),)
            else:
                args = ()
        elif attype in ('interval', 'interval year to month',
                            'interval day to second'):
            if charlen:
                kwargs['precision'] = int(charlen)
            args = ()
        elif charlen:
            args = (int(charlen),)

        while True:
            if attype in self.ischema_names:
                coltype = self.ischema_names[attype]
                break
            else:
                coltype = None
                break

        if coltype:
            coltype = coltype(*args, **kwargs)
        else:
            util.warn("Did not recognize type '%s' of column '%s'" %
                      (attype, name))
            coltype = sqltypes.NULLTYPE
        # adjust the default value
        autoincrement = False
        if default is not None:
            match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
            if match is not None:
                autoincrement = True
                # the default is related to a Sequence
                sch = schema
                if '.' not in match.group(2) and sch is not None:
                    # unconditionally quote the schema name.  this could
                    # later be enhanced to obey quoting rules /
                    # "quote schema"
                    default = match.group(1) + \
                                ('"%s"' % sch) + '.' + \
                                match.group(2) + match.group(3)

        column_info = dict(name=name, type=coltype, nullable=nullable,
                           default=default, autoincrement=autoincrement)
        return column_info

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        raise NotImplementedError()

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        if schema is not None:
            raise NotImplementedError("remote schemas")
        else:
            schema = self.default_schema_name

        FK_SQL = """
            SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
            FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                ON tc.schema_name=kcu.schema_name AND tc.table_name=kcu.table_name
                WHERE tc.schema_name=:schema
                AND tc.table_name=:table
                ORDER BY kcu.ordinal_position
        """

        t = sql.text(FK_SQL)
        c = connection.execute(t, table=table_name, schema=schema)
        fkeys = collections.defaultdict(list)

        # TODO: this is way too simplistic, can't get referents here.
        # might need to regexp a CREATE TABLE statement or similar here.
        for conname, contype, colname in c.fetchall():
            fkeys[conname].append(colname)

        return [
            {
                'name':name,
                'constrained_columns':fkeys[name]
            } for name in fkeys
        ]

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        raise NotImplementedError()
