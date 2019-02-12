import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from orm import metadata
from domain_model import Order


@pytest.fixture
def db():
    # engine = create_engine('sqlite:///:memory:', echo=True)
    engine = create_engine('sqlite:///:memory:')
    metadata.create_all(engine)
    return engine

@pytest.fixture
def session(db):
    return sessionmaker(bind=db)()

def test_smoke(session):
    session.execute('INSERT INTO "order" VALUES (1)')
    session.execute('INSERT INTO "order_lines" VALUES (1, "sku1", 12)')
    session.execute('INSERT INTO "order_lines" VALUES (1, "sku2", 13)')
    r = session.execute('SELECT * from "order" JOIN "order_lines"')
    assert list(r) == [
        (1, 1, "sku1", 12),
        (1, 1, "sku2", 13),
    ]

def test_order_mapper_no_lines(session):
    order = Order({})
    session.add(order)
    assert session.query(Order).first() == order

def test_order_mapper_can_load_lines(session):
    session.execute('INSERT INTO "order" VALUES (1)')
    session.execute('INSERT INTO "order" VALUES (2)')
    session.execute('INSERT INTO "order_lines" VALUES (1, "sku1", 12)')
    session.execute('INSERT INTO "order_lines" VALUES (1, "sku2", 13)')
    session.execute('INSERT INTO "order_lines" VALUES (2, "sku3", 14)')
    expected_order = Order({'sku1': 12, 'sku2': 13})
    retrieved_order = session.query(Order).first()
    assert retrieved_order.lines == expected_order.lines


def test_order_mapper_can_save_lines(session):
    new_order = Order({'sku1': 12, 'sku2': 13})
    session.add(new_order)
    session.commit()

    rows = list(session.execute('SELECT * FROM "order_lines"'))
    assert rows == [
        (1, 'sku1', 12),
        (1, 'sku2', 13),
    ]

def test_order_mapper_can_edit_lines(session):
    session.execute('INSERT INTO "order" VALUES (1)')
    session.execute('INSERT INTO "order" VALUES (2)')
    session.execute('INSERT INTO "order_lines" VALUES (1, "sku1", 12)')
    session.execute('INSERT INTO "order_lines" VALUES (1, "sku2", 13)')
    session.execute('INSERT INTO "order_lines" VALUES (2, "sku3", 14)')

    order = session.query(Order).first()
    order._lines['sku4'] = 99
    session.add(order)
    session.commit()

    rows = list(session.execute('SELECT * FROM "order_lines" WHERE order_id=1'))
    assert rows == [
        (1, 'sku1', 12),
        (1, 'sku2', 13),
        (1, 'sku4', 13),
    ]