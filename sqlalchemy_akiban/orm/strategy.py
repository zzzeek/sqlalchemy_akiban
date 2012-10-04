from sqlalchemy.orm.strategies import AbstractRelationshipLoader
from sqlalchemy.orm.strategies import LazyLoader
from sqlalchemy.orm.strategies import _factory
from sqlalchemy.orm import interfaces
from sqlalchemy.orm import attributes
from sqlalchemy.orm import loading
from sqlalchemy.orm import util as orm_util
from sqlalchemy.sql import util as sql_util
from sqlalchemy import util
from sqlalchemy import log
from sqlalchemy import select
from sqlalchemy import exc as sa_exc

class NestedLoader(AbstractRelationshipLoader):
    def __init__(self, parent):
        super(NestedLoader, self).__init__(parent)
        self.join_depth = self.parent_property.join_depth

    def init_class_attribute(self, mapper):
        self.parent_property.\
            _get_strategy(LazyLoader).init_class_attribute(mapper)

    def setup_query(self, context, entity, path, adapter, \
                                column_collection=None,
                                parentmapper=None,
                                **kwargs):

        if not context.query._enable_eagerloads:
            return

        path = path[self.key]

        with_polymorphic = None

        # if not via query option, check for
        # a cycle
        if not path.contains(context, "loaderstrategy"):
            if self.join_depth:
                if path.length / 2 > self.join_depth:
                    return
            elif path.contains_mapper(self.mapper):
                return

        #if parentmapper is None:
        #    localparent = entity.mapper
        #else:
        #    localparent = parentmapper

        source_selectable = entity.selectable

        with_poly_info = path.get(
            context,
            "path_with_polymorphic",
            None
        )
        if with_poly_info is not None:
            with_polymorphic = with_poly_info.with_polymorphic_mappers
        else:
            with_polymorphic = None

        pj, sj, source, dest, secondary, target_adapter = \
            self.parent_property._create_joins(dest_polymorphic=True,
                    source_selectable=source_selectable)

        add_to_collection = []

        path = path[self.mapper]
        for value in self.mapper._iterate_polymorphic_properties(
                                mappers=with_polymorphic):
            value.setup(
                context,
                entity,
                path,
                None,
                parentmapper=self.mapper,
                column_collection=add_to_collection)

        context.secondary_columns.append(
            select(add_to_collection).where(pj).as_scalar()
        )

log.class_logger(NestedLoader)

_factory["akiban_nested"] = NestedLoader
