from sqlalchemy import Table, Column, Integer, String, Numeric, ForeignKey

def cust_order_item(metadata):
    Table('customer',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(20)),
    )

    Table('order',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('customer_id', Integer, ForeignKey('customer.id')),
        Column('order_info', String(20)),
    )

    Table('item',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('order_id', Integer, ForeignKey('order.id')),
        Column('price', Numeric(10, 2)),
        Column('quanity', Integer)
    )

