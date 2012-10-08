from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_, is_, AssertsCompiledSQL, AssertsExecutionResults
from sqlalchemy.orm import relationship, Session, mapper
from .fixtures import cust_order_item, cust_order_data

from sqlalchemy_akiban import orm

class _Fixture(object):
    @classmethod
    def define_tables(cls, metadata):
        cust_order_item(metadata)

    @classmethod
    def setup_classes(cls):
        class Customer(cls.Comparable):
            pass
        class Order(cls.Comparable):
            pass
        class Item(cls.Comparable):
            pass

    @classmethod
    def setup_mappers(cls):
        Customer, Order, Item = cls.classes.Customer, \
                                    cls.classes.Order, \
                                    cls.classes.Item
        customer, order, item = cls.tables.customer,\
                                    cls.tables.order,\
                                    cls.tables.item
        mapper(Customer, customer, properties={
            'orders': relationship(Order, backref="customer")
        })
        mapper(Order, order, properties={
            'items': relationship(Item, backref="order")
        })
        mapper(Item, item)

class RenderTest(_Fixture, fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = 'akiban'

    def test_option_creation(self):
        from sqlalchemy.orm.strategies import EagerLazyOption
        Customer = self.classes.Customer
        opt = orm.nestedload(Customer.orders)
        assert isinstance(opt, EagerLazyOption)
        is_(opt.key[0], Customer.orders)

    def test_render_basic_nested(self):
        Customer = self.classes.Customer
        s = Session()
        q = s.query(Customer).options(orm.nestedload(Customer.orders))
        self.assert_compile(
                q,
                'SELECT customer.id AS customer_id, '
                'customer.name AS customer_name, (SELECT "order".id, '
                '"order".customer_id, "order".order_info FROM "order" '
                'WHERE customer.id = "order".customer_id) AS anon_1 '
                'FROM customer'
        )

class LoadTest(_Fixture, fixtures.MappedTest, AssertsExecutionResults):
    run_inserts = 'once'

    @classmethod
    def insert_data(cls):
        cust_order_data(cls)

    def test_load_collection(self):
        Customer = self.classes.Customer
        Order = self.classes.Order

        s = Session()
        q = s.query(Customer).options(orm.nestedload(Customer.orders)).filter(Customer.id == 1)

        with self.assert_statement_count(1):
            eq_(
                q.all(),
                [Customer(id=1, name='David McFarlane',
                        orders=[Order(customer_id=1, id=101,
                                    order_info='apple related'),
                                Order(customer_id=1, id=102,
                                    order_info='apple related'),
                                Order(customer_id=1, id=103,
                                    order_info='apple related')]
                        )]
            )


