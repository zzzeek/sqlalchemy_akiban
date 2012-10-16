from sqlalchemy.orm import strategies as _sqla_strat
from ..dialect.base import NestedResult as _NestedResult, nested as _nested

# bootstraps the "strategy" plugins
from . import strategy

class ORMNestedResult(_NestedResult):
    hashable = False

    def __init__(self, query):
        self.query = query

    def akiban_result_processor(self, gen_nested_context):
        super_process = super(ORMNestedResult, self).\
            akiban_result_processor(gen_nested_context)
        def process(value):
            cursor = super_process(value)
            return list(
                self.query.instances(cursor)
            )
        return process

class orm_nested(_nested):

    def __init__(self, query):
        stmt = query.statement
        super(orm_nested, self).__init__(stmt)
        self.type = ORMNestedResult(query)


def nestedload(*keys, **kw):
    return _sqla_strat.EagerLazyOption(keys, lazy='akiban_nested')

def nestedload_all(*keys, **kw):
    return _sqla_strat.EagerLazyOption(keys, lazy='akiban_nested', chained=True)

