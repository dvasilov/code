import datetime
import random
import time
import uuid

import psycopg2
import multiprocessing as mp


def get_conn_and_cursor():
    conn = psycopg2.connect(
        dbname="allocation",
        user="allocation",
        host="localhost",
        port="54321",
        password="abc123"
    )
    cur = conn.cursor()
    return conn, cur


def get_random_batch_row():
    ref = str(uuid.uuid4())
    sku = 'bench-' + str(uuid.uuid4())
    eta = datetime.datetime.now() + datetime.timedelta(days=random.randint(0, 1000))
    eta = eta.utcnow()
    _purchase_quantity = random.randint(1, 1000)
    return ref, sku, _purchase_quantity, eta


def fill_database_with_fake_batches(con, cur, number_of_batches=1000):
    batch_insert_query = "INSERT into batches_benchmark (reference, sku, _purchased_quantity, eta) VALUES ('%s', '%s', '%s', '%s')"

    for _ in range(number_of_batches):
        batch = get_random_batch_row()
        cur.execute(batch_insert_query % batch)

    con.commit()


def drop_data_from_db(con, cur):
    drop_bench_batches_query = "DELETE from batches_benchmark where sku like 'bench-%'"
    cur.execute(drop_bench_batches_query)


def prepare_database():
    con, cur = get_conn_and_cursor()
    create_table(con, cur)
    fill_database_with_fake_batches(con, cur)
    con.close()


def clean_up():
    con, cur = get_conn_and_cursor()
    drop_data_from_db(con, cur)
    drop_table(con, cur)


def check_time(uow):
    start_time = time.time()
    processes = [mp.Process(target=update_batches(uow)) for x in range(1, 5)]
    for p in processes:
        p.start()

    for p in processes:
        p.join()

    end_time = time.time() - start_time
    return end_time


def update_batches(uow):
    refs = get_benchmark_refs()
    for _ in range(10000):
        ref = random.choice(refs)
        update_batch(ref, uow)


def get_benchmark_refs():
    con, cur = get_conn_and_cursor()
    cur.execute("SELECT reference from batches_benchmark where sku like 'bench-%'")
    references = cur.fetchall()
    con.close()
    return references


def update_batch(ref, uow):
    with uow:
        batch = uow.batches.get(ref)
        batch._purchased_quantity = batch._purchased_quantity + 1
        uow.session.add(batch)
        uow.commit()


def create_table(con, cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS batches_benchmark (
    id SERIAL primary key,
    reference VARCHAR (255) unique,
    sku VARCHAR (255),
    _purchased_quantity int,
    eta date);
    """)
    con.commit()


def drop_table(con, cur):
    cur.execute("DROP TABLE IF EXISTS batches_benchmark")
    con.commit()