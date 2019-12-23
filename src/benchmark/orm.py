import logging

from allocation import model
from sqlalchemy import Table, MetaData, Column, Integer, String, Date

from sqlalchemy.orm import mapper

logger = logging.getLogger(__name__)

metadata = MetaData()

batches = Table(
    'batches_benchmark', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('reference', String(255)),
    Column('sku', String(255)),
    Column('_purchased_quantity', Integer, nullable=False),
    Column('eta', Date, nullable=True),
)


def start_mappers():
    logger.info("Starting mappers")
    mapper(model.Batch, batches)