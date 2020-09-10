
from allocation import config
from allocation.unit_of_work import AbstractUnitOfWork
from benchmark.repository import BatchRepository, BatchWithForUpdateRepository
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

WITH_FOR_UPDATE__SESSION_FACTORY = sessionmaker(bind=create_engine(
    config.get_postgres_uri(),
))

SERIALIZABLE_ISOLATION_SESSION_FACTORY = sessionmaker(bind=create_engine(
    config.get_postgres_uri(),
    isolation_level="SERIALIZABLE",
))


class UnitOfWorkBenchmark(AbstractUnitOfWork):

    def __init__(self, session_factory):
        self.session_factory = session_factory

    def __exit__(self, *args):
        super().__exit__(*args)
        self.session.close()

    def _commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def commit(self):
        self._commit()


class IsolationUnitOfWork(UnitOfWorkBenchmark):

    def __init__(self, session_factory=SERIALIZABLE_ISOLATION_SESSION_FACTORY):
        self.session_factory = session_factory
        super().__init__(session_factory)

    def __enter__(self):
        self.session = self.session_factory()  # type: Session
        self.batches = BatchRepository(self.session)
        return super().__enter__()


class WithForUpdateUnitOfWork(UnitOfWorkBenchmark):
    def __init__(self, session_factory=WITH_FOR_UPDATE__SESSION_FACTORY):
        self.session_factory = session_factory
        super().__init__(session_factory)

    def __enter__(self):
        self.session = self.session_factory()  # type: Session
        self.batches = BatchWithForUpdateRepository(self.session)
        return super().__enter__()
