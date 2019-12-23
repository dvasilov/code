import time

from benchmark.orm import start_mappers
from benchmark.utils import prepare_database, clean_up, check_time
from benchmark.unit_of_work import IsolationUnitOfWork, WithForUpdateUnitOfWork

if __name__ == "__main__":

    # first we will prepare new table
    start_mappers()
    isolation_uow = IsolationUnitOfWork()
    with_update_uow = WithForUpdateUnitOfWork()

    prepare_database()

    with_for_update_time = check_time(with_update_uow)
    print("with_for_update_time ", with_for_update_time)

    isolation_time = check_time(isolation_uow)
    print("isolation_time ", isolation_time)

    clean_up()


"""
with_for_update_time  210.55391550064087
isolation_time  202.72578072547913

with_for_update_time  200.93747401237488
isolation_time  218.2748246192932

with_for_update_time  213.71013808250427
isolation_time  206.7240309715271

"""