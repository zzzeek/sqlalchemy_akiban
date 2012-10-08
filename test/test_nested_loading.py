from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import eq_, is_, AssertsCompiledSQL, \
                AssertsExecutionResults
from sqlalchemy.orm import relationship, Session, mapper, \
            immediateload, backref
from .fixtures import cust_order_item, cust_order_data
from decimal import Decimal
from sqlalchemy_akiban import orm

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

    def test_subquery_no_eagers(self):
        Customer = self.classes.Customer
        s = Session()
        stmt = s.query(Customer).options(orm.nestedload(Customer.orders)).subquery()
        self.assert_compile(
                stmt,
                'SELECT customer.id, customer.name FROM customer'
        )

    def test_statement(self):
        Customer = self.classes.Customer
        s = Session()
        # there's an annotate in here which needs to succeed
        stmt = s.query(Customer).options(orm.nestedload(Customer.orders)).statement
        self.assert_compile(
                stmt,
                'SELECT customer.id, customer.name, (SELECT "order".id, '
                '"order".customer_id, "order".order_info FROM "order" '
                'WHERE customer.id = "order".customer_id) AS anon_1 FROM customer'
        )

class LoadTest(_Fixture, fixtures.MappedTest, AssertsExecutionResults):
    run_inserts = 'once'
    run_deletes = None



    def test_load_collection(self):
        Customer = self.classes.Customer

        s = Session()
        q = s.query(Customer).options(orm.nestedload(Customer.orders)).\
                            filter(Customer.id == 1)

        with self.assert_statement_count(1):
            eq_(
                q.all(),
                [self._orm_fixture(orders=True)]
            )

    def test_refresh_collection(self):
        Customer = self.classes.Customer

        s = Session()
        customers = s.query(Customer).options(immediateload(Customer.orders)).\
                            filter(Customer.id == 1).all()
        orders = customers[0].orders

        # expire...
        for ord_ in orders:
            s.expire(ord_)

        # + load again, should refresh Orders on Customer
        s.query(Customer).options(orm.nestedload(Customer.orders)).\
                            filter(Customer.id == 1).all()

        # covers the "exec" loader
        with self.assert_statement_count(0):
            eq_(
                customers,
                [self._orm_fixture(orders=True)]
            )

    def test_load_scalar(self):
        Order = self.classes.Order

        s = Session()
        q = s.query(Order).options(orm.nestedload(Order.customer)).\
                        filter(Order.id == 102)

        order = self._orm_fixture(orders=True).orders[1]

        # avoid comparison/lazy load of 'orders' on the customer
        del order.customer.__dict__['orders']
        with self.assert_statement_count(1):
            eq_(
                q.all(),
                [order]
            )

    def test_refresh_scalar(self):
        Order = self.classes.Order

        s = Session()
        q = s.query(Order).options(orm.nestedload(Order.customer)).\
                        filter(Order.id == 102)
        order = q.all()
        customer = order[0].customer

        # expire...
        s.expire(customer)

        # + load again, should refresh Customer on Order
        s.query(Order).options(orm.nestedload(Order.customer)).\
                        filter(Order.id == 102).all()

        fixture_order = self._orm_fixture(orders=True).orders[1]

        # avoid comparison/lazy load of 'orders' on the customer
        del fixture_order.customer.__dict__['orders']
        with self.assert_statement_count(0):
            eq_(
                fixture_order,
                order[0]
            )

    def test_double_nesting(self):
        Customer = self.classes.Customer
        Order = self.classes.Order

        s = Session()
        q = s.query(Customer).options(
                        orm.nestedload_all(Customer.orders, Order.items)).\
                            filter(Customer.id == 1)

        with self.assert_statement_count(1):
            eq_(
                q.all(),
                [self._orm_fixture(orders=True, items=True)]
            )

class MappedWNestTest(_Fixture, fixtures.MappedTest, AssertsExecutionResults):
    lazy = 'akiban_nested'
    run_inserts = 'once'
    run_deletes = None

    def test_load_collection(self):
        Customer = self.classes.Customer

        s = Session()
        q = s.query(Customer).filter(Customer.id == 1)

        with self.assert_statement_count(1):
            eq_(
                q.all(),
                [self._orm_fixture(orders=True)]
            )

    def test_double_nesting(self):
        Customer = self.classes.Customer

        s = Session()
        q = s.query(Customer).filter(Customer.id == 1)

        with self.assert_statement_count(1):
            eq_(
                q.all(),
                [self._orm_fixture(orders=True, items=True)]
            )

class RecursionOverflowTest(_Fixture, fixtures.MappedTest, AssertsExecutionResults):
    run_inserts = 'once'
    run_deletes = None
    run_setup_mappers = 'each'

    @classmethod
    def setup_mappers(cls):
        pass

    def _fixture(self, join_depth):
        Customer, Order = self.classes.Customer, \
                                    self.classes.Order
        customer, order = self.tables.customer,\
                                    self.tables.order
        mapper(Customer, customer, properties={
            'orders': relationship(Order,
                        backref=backref("customer",
                                lazy='akiban_nested', join_depth=join_depth),
                    lazy='akiban_nested', join_depth=join_depth)
        })
        mapper(Order, order)
        return Customer, Order

    def test_no_overflow_stop_depth(self):
        Customer, Order = self._fixture(1)

        s = Session()
        q = s.query(Customer).filter(Customer.id == 1)

        with self.assert_statement_count(1):
            eq_(
                q.all(),
                [self._orm_fixture(orders=True)]
            )

    def test_no_overflow_stop_natural(self):
        Customer, Order = self._fixture(None)

        s = Session()
        q = s.query(Customer).filter(Customer.id == 1)

        with self.assert_statement_count(1):
            eq_(
                q.all(),
                [self._orm_fixture(orders=True)]
            )

