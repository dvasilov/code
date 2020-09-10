import datetime
import multiprocessing as mp
import random
import time
import uuid

import psycopg2
from pprint import pprint


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


def get_random_batch_row(skus):
    ref = str(uuid.uuid4())
    sku = random.choice(skus)
    eta = datetime.datetime.now() + datetime.timedelta(days=random.randint(0, 1000))
    eta = eta.utcnow()
    _purchase_quantity = random.randint(1, 1000)
    return ref, sku, _purchase_quantity, eta


def fill_database_with_fake_batches(con, cur, number_of_batches=10000):
    batch_insert_query = "INSERT into batches_benchmark (reference, sku, _purchased_quantity, eta) VALUES ('%s', '%s', '%s', '%s')"
    skus = ["bench-" + str(uuid.uuid4()) for _ in range(100)]
    for _ in range(number_of_batches):
        batch = get_random_batch_row(skus)
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


def check_time(skus, uow):
    start_time = time.time()
    manager = mp.Manager()
    errors_count = manager.list()

    processes = [mp.Process(target=update_batches, args=(skus, uow, errors_count)) for _ in range(1, 10)]
    for p in processes:
        p.start()

    for p in processes:
        p.join()

    end_time = time.time() - start_time
    return end_time, errors_count


def get_benchmark_refs():
    con, cur = get_conn_and_cursor()
    cur.execute("SELECT reference from batches_benchmark where sku like 'bench-%'")
    references = cur.fetchall()
    con.close()
    return references


def get_skus():
    con, cur = get_conn_and_cursor()
    cur.execute("SELECT sku from batches_benchmark where sku like 'bench-%'")
    skus = cur.fetchall()
    con.close()
    return skus


def update_batches(skus, uow, errors_count):
    # this will be all number of errors for 1 process
    errors = 0
    for _ in range(100):
        with uow:
            # we want to count only those errors that were not fixed by retries
            sku_error = 0
            sku = random.choice(skus)
            batches = uow.batches.get_by_sku(sku)
            wait_time = 1
            for batch in batches:
                batch._purchased_quantity = batch._purchased_quantity + 1
                uow.session.add(batch)
            for _ in range(10):
                try:
                    uow.commit()
                except Exception as e:
                    sku_error += 1
                    time.sleep(wait_time)
                else:
                    # retry fix commit problem so decrease errors cound for this iteration
                    sku_error = 0
                    break

        errors += sku_error
    if errors:
        errors_count.append((errors, len(batches)))


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
