from . import strategy
from sqlalchemy.orm import strategies as _sqla_strat

def nestedload(*keys, **kw):
    return _sqla_strat.EagerLazyOption(keys, lazy='akiban_nested')
