from sqlalchemy.testing import fixtures
from .fixtures import cust_order_item
from sqlalchemy import select
from sqlalchemy_akiban import nested
from sqlalchemy.testing.assertions import AssertsCompiledSQL, eq_

class NestedSelectableTest(fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = 'akiban'

    run_create_tables = None
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        cust_order_item(metadata)
        metadata.bind = None

    def test_basic(self):
        customer = self.tables.customer
        order = self.tables.order

        sub_stmt = nested(select([order]).where(order.c.customer_id
                                            == customer.c.id)).label('o')
        stmt = select([sub_stmt]).where(customer.c.id == 1)
        self.assert_compile(
            stmt,
            'SELECT (SELECT "order".id, "order".customer_id, '
            '"order".order_info FROM "order" WHERE "order".customer_id = '
            'customer.id) AS o FROM customer WHERE customer.id = %(id_1)s'
        )

    def test_double(self):
        customer = self.tables.customer
        order = self.tables.order
        item = self.tables.item

        sub_sub_stmt = nested(select([item]).where(item.c.order_id ==
                                            order.c.id)).label('i')
        sub_stmt = nested(select([sub_sub_stmt]).where(order.c.customer_id ==
                                            customer.c.id)).label('o')
        stmt = select([sub_stmt]).where(customer.c.id == 1)

        self.assert_compile(
            stmt,
            'SELECT (SELECT (SELECT item.id, item.order_id, item.price, '
            'item.quantity FROM item WHERE item.order_id = "order".id) AS i '
            'FROM "order" WHERE "order".customer_id = customer.id) AS o '
            'FROM customer WHERE customer.id = %(id_1)s'
        )

    def test_str_via_default_compiler(self):
        customer = self.tables.customer
        order = self.tables.order

        sub_stmt = nested(select([order]).where(order.c.customer_id
                                            == customer.c.id)).label('o')
        stmt = select([sub_stmt]).where(customer.c.id == 1)
        eq_(
            str(stmt).replace("\n", ""),
            'SELECT (SELECT "order".id, "order".customer_id, "order".order_info '
            'FROM "order" WHERE "order".customer_id = customer.id) AS o '
            'FROM customer WHERE customer.id = :id_1'
        )
