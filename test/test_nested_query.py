from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_, is_, AssertsCompiledSQL, \
                AssertsExecutionResults
from sqlalchemy.orm import relationship, Session, mapper, \
            immediateload, backref
from sqlalchemy.sql import select
from .fixtures import cust_order_item, cust_order_data
from decimal import Decimal
from sqlalchemy_akiban import orm, nested


class _Fixture(object):
    @classmethod
    def define_tables(cls, metadata):
        cust_order_item(metadata)

    @classmethod
    def insert_data(cls):
        cust_order_data(cls)

    @classmethod
    def setup_classes(cls):
        class Customer(cls.Comparable):
            pass
        class Order(cls.Comparable):
            pass
        class Item(cls.Comparable):
            pass

    lazy = True

    @classmethod
    def setup_mappers(cls):
        Customer, Order, Item = cls.classes.Customer, \
                                    cls.classes.Order, \
                                    cls.classes.Item
        customer, order, item = cls.tables.customer,\
                                    cls.tables.order,\
                                    cls.tables.item
        mapper(Customer, customer, properties={
            'orders': relationship(Order, backref="customer", lazy=cls.lazy)
        })
        mapper(Order, order, properties={
            'items': relationship(Item, backref="order", lazy=cls.lazy)
        })
        mapper(Item, item)

    def _orm_fixture(self, orders=False, items=False):
        Customer = self.classes.Customer
        Order = self.classes.Order
        Item = self.classes.Item

        c1 = Customer(id=1, name='David McFarlane')
        if orders:
            c1.orders = [Order(customer_id=1, id=101,
                            order_info='apple related'),
                        Order(customer_id=1, id=102,
                            order_info='apple related'),
                        Order(customer_id=1, id=103,
                            order_info='apple related')]
        if items:
            (1001, 101, 9.99, 1),
            (1002, 101, 19.99, 2),
            (1003, 102, 9.99, 1),
            (1004, 103, 9.99, 1),

            c1.orders[0].items = [
                Item(id=1001, order_id=101, price=Decimal("9.99"), quantity=1),
                Item(id=1002, order_id=101, price=Decimal("19.99"), quantity=2),
            ]
            c1.orders[1].items = [
                Item(id=1003, order_id=102, price=Decimal("9.99"), quantity=1),
            ]
            c1.orders[2].items = [
                Item(id=1004, order_id=103, price=Decimal("9.99"), quantity=1),
            ]

        return c1

class RenderTest(_Fixture, fixtures.MappedTest, AssertsCompiledSQL):
    __dialect__ = 'akiban'


    def test_render_nested_alone(self):
        Customer = self.classes.Customer
        Order = self.classes.Order

        n = nested(Order).where(Customer.orders)
        self.assert_compile(
            n,
            '(SELECT "order".id, "order".customer_id, "order".order_info '
            'FROM "order", customer WHERE customer.id = "order".customer_id)'
        )

    def test_render_basic_nested(self):
        Customer = self.classes.Customer
        Order = self.classes.Order
        s = Session()
        n = nested(Order).where(Customer.orders)
        q = s.query(Customer, n)
        self.assert_compile(
                q,
                'SELECT customer.id AS customer_id, '
                'customer.name AS customer_name, (SELECT "order".id, '
                '"order".customer_id, "order".order_info FROM "order" '
                'WHERE customer.id = "order".customer_id) AS anon_1 '
                'FROM customer'
        )


