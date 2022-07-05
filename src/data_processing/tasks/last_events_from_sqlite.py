#!/usr/bin/python3
import pathlib
import time
import logging
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
import sqlite3
import os
import pandas as pd
from datetime import datetime 

current_path = pathlib.Path(__file__).parent.resolve()

class Pipeline:

    def __init__(self, db_name=None, clean_db=False):
        if db_name is None:
            self._db_path = f'{current_path}/../../../db/local_db.db'
        else:
            self._db_path = f'/tmp/{db_name}'
        self._db_name = f'sqlite:////{self._db_path}'
        logging.warning(f"Using local sqlite3 db='{self._db_name}'")
        self._db_engine = create_engine(
            self._db_name,
            poolclass=StaticPool, 
            connect_args={"check_same_thread": False},
            echo=True
        )


    def run(self, start_date_str:str, end_date_str:str=None, events_cnt:int=10):
        """
        Get the last N events given a date_str

        """
        date_fmt ="%Y-%m-%d"
        date_time_obj = datetime.strptime(start_date_str, date_fmt)
        start_unixtime = time.mktime(date_time_obj.timetuple())    
        cols = 'launch, latitude, longitude, EPOCH, EPOCH_UNIX_TIME'
        sql = f'SELECT {cols} FROM starlink WHERE EPOCH_UNIX_TIME >= {start_unixtime} ORDER BY EPOCH DESC LIMIT {events_cnt}'
        if end_date_str is not None:
            date_time_obj = datetime.strptime(end_date_str, date_fmt)
            end_unixtime = time.mktime(date_time_obj.timetuple())
            sql = f'SELECT {cols} FROM starlink WHERE EPOCH_UNIX_TIME >= {start_unixtime} AND EPOCH_UNIX_TIME <= {end_unixtime} ORDER BY EPOCH DESC LIMIT {events_cnt}'
            logging.warning(f"Get last {events_cnt} event since between '{start_date_str}' and '{end_date_str}'")
        else:
            logging.warning(f"Get last {events_cnt} event since T='{start_date_str}'")
        result = self._db_engine.execute(sql)
        # logging.info(list(result))
        sql = f'SELECT count(*) FROM starlink'
        # logging.info(sql)
        total_result = list(self._db_engine.execute(sql))
        logging.warning(f"starlink table rows count is {total_result}")
        sql = f'SELECT max(EPOCH), max(EPOCH_UNIX_TIME), min(EPOCH), min(EPOCH_UNIX_TIME) FROM starlink'
        epoch_range = list(self._db_engine.execute(sql))
        logging.warning(f"starlink table rows EPOCH range is {epoch_range}")
        return list(result)
