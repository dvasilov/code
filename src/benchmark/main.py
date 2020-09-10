import time

from benchmark.orm import start_mappers
from benchmark.utils import prepare_database, clean_up, check_time, get_skus
from benchmark.unit_of_work import IsolationUnitOfWork, WithForUpdateUnitOfWork

if __name__ == "__main__":

    # first we will prepare new table
    start_mappers()
    isolation_uow = IsolationUnitOfWork()
    with_update_uow = WithForUpdateUnitOfWork()

    prepare_database()
    skus = get_skus()
    with_for_update_time, with_for_update_errors = check_time(skus, with_update_uow)

    isolation_time,  isolation_errors = check_time(skus, isolation_uow)

    print("\n" * 10)
    print(f"with_for_update_time : {with_for_update_time} skus_with_errors_count: {len(with_for_update_errors)}"
          f" total_errors: {sum([x[0] for x in with_for_update_errors])}")
    print(f"isolation_time : {isolation_time}; skus_with_errors_count: {len(isolation_errors)}"
          f" total_errors: {sum([x[0] for x in isolation_errors])}")

    # Lets see percentage of time getting for bench
    print(f"Percent of time: {(isolation_time * 100) / with_for_update_time}")
    clean_up()


"""

with_for_update_time : 4.8201072216033936 skus_with_errors_count: 9 total_errors: 0
isolation_time : 3.593703269958496; skus_with_errors_count: 9 total_errors: 801
Percent of time: 74.55650060753341
"""