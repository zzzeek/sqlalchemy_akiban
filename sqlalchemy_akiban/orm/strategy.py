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
from .. import nested

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

        mapper_path = path[self.mapper]
        for value in self.mapper._iterate_polymorphic_properties(
                                mappers=with_polymorphic):
            value.setup(
                context,
                entity,
                mapper_path,
                None,
                parentmapper=self.mapper,
                column_collection=add_to_collection)

        our_col = nested(select(add_to_collection).where(pj).as_scalar())
        path.set(context, "nested_result", our_col)
        context.secondary_columns.append(
            our_col
        )

    def create_row_processor(self, context, path, mapper, row, adapter):
        if not self.parent.class_manager[self.key].impl.supports_population:
            raise sa_exc.InvalidRequestError(
                        "'%s' does not support object "
                        "population - eager loading cannot be applied." %
                        self)

        path = path[self.key]

        our_col = path.get(context, "nested_result")
        if our_col in row:
            _instance = loading.instance_processor(
                                self.mapper,
                                context,
                                path[self.mapper],
                                None)

            if not self.uselist:
                return self._create_scalar_loader(context, self.key, our_col, _instance)
            else:
                return self._create_collection_loader(context, self.key, our_col, _instance)

        else:
            return self.parent_property.\
                            _get_strategy(LazyLoader).\
                            create_row_processor(
                                            context, path,
                                            mapper, row, adapter)


    def _create_collection_loader(self, context, key, our_col, _instance):
        def load_collection_from_joined_new_row(state, dict_, row):
            collection = attributes.init_state_collection(
                                            state, dict_, key)
            result_list = util.UniqueAppender(collection,
                                              'append_without_event')
            context.attributes[(state, key)] = result_list
            for nested_row in row[our_col]:
                _instance(nested_row, result_list)

        def load_collection_from_joined_exec(state, dict_, row):
            for nested_row in row[our_col]:
                _instance(nested_row, None)

        return load_collection_from_joined_new_row, \
                None, \
                None, load_collection_from_joined_exec

    def _create_scalar_loader(self, context, key, _instance):
        def load_scalar_from_joined_new_row(state, dict_, row):
            # set a scalar object instance directly on the parent
            # object, bypassing InstrumentedAttribute event handlers.
            dict_[key] = _instance(row, None)

        def load_scalar_from_joined_existing_row(state, dict_, row):
            # call _instance on the row, even though the object has
            # been created, so that we further descend into properties
            existing = _instance(row, None)
            if existing is not None \
                and key in dict_ \
                and existing is not dict_[key]:
                util.warn(
                    "Multiple rows returned with "
                    "uselist=False for eagerly-loaded attribute '%s' "
                    % self)

        def load_scalar_from_joined_exec(state, dict_, row):
            _instance(row, None)

        return load_scalar_from_joined_new_row, \
                load_scalar_from_joined_existing_row, \
                None, load_scalar_from_joined_exec

log.class_logger(NestedLoader)

_factory["akiban_nested"] = NestedLoader
