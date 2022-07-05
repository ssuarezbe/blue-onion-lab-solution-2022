#!/usr/bin/python3
from sqlalchemy.types import UnicodeText
from sqlalchemy.pool import StaticPool
from sqlalchemy import Column, Integer, ForeignKey, Table, Numeric, JSON
from datetime import datetime 
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.declarative import declarative_base
from data_processing.tasks.utils import set_epoch_unix_time

Base = declarative_base()

class StarLink(Base):

    __tablename__ = "starlink"

    id = Column(Integer, primary_key=True)
    version = Column('version', UnicodeText, nullable=False)
    launch = Column('launch', UnicodeText, nullable=False)
    longitude = Column('longitude', Numeric, nullable=True)
    latitude = Column('latitude', Numeric, nullable=True)
    object_id = Column('OBJECT_ID', UnicodeText, nullable=False)
    object_name = Column('OBJECT_NAME', UnicodeText, nullable=False)
    # time columns
    creation_date = Column('CREATION_DATE', UnicodeText, nullable=False)
    time_system = Column('TIME_SYSTEM', UnicodeText, nullable=False)
    epoch = Column('EPOCH', UnicodeText, nullable=False)
    _epoch_unix_time = Column('EPOCH_UNIX_TIME', Numeric, nullable=False)
    launch_date = Column('LAUNCH_DATE', UnicodeText, nullable=False )


    @hybrid_property
    def epoch_unix_time(self):
        return self._epoch_unix_time

    @epoch_unix_time.setter
    def set_epoch_unix_time(self):
        self._epoch_unix_time = set_epoch_unix_time(self.epoch)


class StarLinkRaw(Base):

    __tablename__ = "starlink_raw"
    id = Column(Integer, primary_key=True)
    raw_json = Column('raw_json',JSON, nullable=False)
    launch = Column('launch', UnicodeText, nullable=False)
