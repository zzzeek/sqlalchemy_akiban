from sqlalchemy import Table, Column, Integer, String, Numeric, ForeignKey
from sqlalchemy.testing import config

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
        Column('quantity', Integer)
    )

def cust_order_data(cls):
    customer = cls.tables.customer
    order = cls.tables.order
    item = cls.tables.item

    config.db.execute(
        customer.insert(),
        [
        {"id":id_, "name":name} for id_, name in
            [(1, 'David McFarlane'),
            (2, 'Ori Herrnstadt'),
            (3, 'Tim Wegner'),
            (4, 'Jack Orenstein'),
            (5, 'Peter Beaman'),
            (6, 'Thomas Jones-Low'),
            (7, 'Mike McMahon'),
            (8, 'Padraig O''Sullivan'),
            (9, 'Yuval Shavit'),
            (10, 'Nathan Williams'),
            (11, 'Chris Ernenwein')]
        ]

    )
    config.db.execute(
        order.insert(),
        [
            {"id":id_, "customer_id":customer_id, "order_info":order_info}
            for id_, customer_id, order_info, dt in
            [
                (101, 1, 'apple related', '2012-09-05 17:24:12'),
                (102, 1, 'apple related', '2012-09-05 17:24:12'),
                (103, 1, 'apple related', '2012-09-05 17:24:12'),
                (104, 2, 'kite', '2012-09-05 17:24:12'),
                (105, 2, 'surfboard', '2012-09-05 17:24:12'),
                (106, 2, 'some order info', '2012-09-05 17:24:12'),
                (107, 3, 'some order info', '2012-09-05 17:24:12'),
                (108, 3, 'some order info', '2012-09-05 17:24:12'),
                (109, 3, 'some order info', '2012-09-05 17:24:12'),
                (110, 4, 'some order info', '2012-09-05 17:24:12'),
                (111, 4, 'some order info', '2012-09-05 17:24:12'),
                (112, 4, 'some order info', '2012-09-05 17:24:12'),
                (113, 5, 'some order info', '2012-09-05 17:24:12'),
                (114, 5, 'some order info', '2012-09-05 17:24:12'),
                (115, 5, 'some order info', '2012-09-05 17:24:12'),
                (116, 6, 'some order info', '2012-09-05 17:24:12'),
                (117, 6, 'some order info', '2012-09-05 17:24:12'),
                (118, 6, 'some order info', '2012-09-05 17:24:12'),
                (119, 7, 'some order info', '2012-09-05 17:24:12'),
                (120, 7, 'some order info', '2012-09-05 17:24:12'),
                (121, 7, 'some order info', '2012-09-05 17:24:12'),
                (122, 8, 'some order info', '2012-09-05 17:24:12'),
                (123, 8, 'some order info', '2012-09-05 17:24:12'),
                (124, 8, 'some order info', '2012-09-05 17:24:12'),
                (125, 9, 'some order info', '2012-09-05 17:24:12'),
                (126, 9, 'some order info', '2012-09-05 17:24:12'),
                (127, 9, 'some order info', '2012-09-05 17:24:12'),
            ]
        ]
    )

    config.db.execute(
        item.insert(),
        [
            {"id":id_, "order_id":order_id, "price":price, "quantity":quantity}
            for id_, order_id, price, quantity in
            [
            (1001, 101, 9.99, 1),
            (1002, 101, 19.99, 2),
            (1003, 102, 9.99, 1),
            (1004, 103, 9.99, 1),
            (1005, 104, 9.99, 5),
            (1006, 105, 9.99, 1),
            (1007, 106, 9.99, 1),
            (1008, 107, 999.99, 1),
            (1009, 107, 9.99, 1),
            (1010, 108, 9.99, 1),
            (1011, 109, 9.99, 1),
            ]
        ]
    )


