#!/usr/bin/python3
import pathlib
from sqlalchemy.orm import sessionmaker
import math
import logging
import data_processing.models as models
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
import sqlite3
import os
import pandas as pd
from data_processing.tasks.utils import set_epoch_unix_time

current_path = pathlib.Path(__file__).parent.resolve()

class Pipeline:

    def __init__(self, db_name=None, clean_db=False):
        if db_name is None:
            self._db_path = f'{current_path}/../../../db/local_db.db'
        else:
            self._db_path = f'/tmp/{db_name}'
            try:
                os.system("sqlite3 {} .quit".format(self._db_path))
            except Exception as e:
                logging.error(e)
                logging.warning("Error during automatic db file creation. Create it manually!")    
            logging.warning(f"DB file created in {self._db_path}")
        self._db_name = f'sqlite:////{self._db_path}'
        logging.warning(f"Using local sqlite3 db='{self._db_name}'")
        self._db_engine = create_engine(
            self._db_name,
            poolclass=StaticPool, 
            connect_args={"check_same_thread": False},
            echo=True
        )
        self._init_sqlite3(clean_db=clean_db)


    def _init_sqlite3(self, clean_db=False):
        logging.warning(sqlite3.version)
        logging.warning(f"Init sqlite3 db {self._db_name}")
        conn = sqlite3.connect(self._db_path)
        c = conn.cursor()
        if clean_db:
            logging.warning("Creating tables....")
            c.execute("DROP TABLE IF EXISTS starlink_raw")
            c.execute("DROP TABLE IF EXISTS starlink")
            c.execute(
                "CREATE TABLE starlink_raw (id INTEGER PRIMARY KEY, "
                "raw_json JSON, launch TEXT)")
            c.execute(
                "CREATE TABLE starlink (id INTEGER PRIMARY KEY, "
                "version TEXT, launch TEXT NOT NULL, longitude NUMERIC, latitude NUMERIC,"
                "OBJECT_ID TEXT NOT NULL, OBJECT_NAME TEXT NOT NULL, CREATION_DATE TEXT NOT NULL, TIME_SYSTEM TEXT NOT NULL,"
                "EPOCH TEXT NOT NULL, EPOCH_UNIX_TIME NUMERIC NOT NULL, LAUNCH_DATE TEXT NOT NULL)")
            conn.commit()
        conn.close()


    def get_cls_colums(self):
        starlink_cols = models.StarLink.__table__.columns.keys()
        logging.info(starlink_cols)
        starlink_cols.remove('id')
        starlink_cols.remove('EPOCH_UNIX_TIME')# Hybrid property
        starlink_raw_cols = models.StarLinkRaw.__table__.columns.keys()
        starlink_raw_cols.remove('id')
        starlink_raw_cols.remove('raw_json')
        return starlink_cols, starlink_raw_cols


    def df_rows_to_sql_cls(self, df, starlink_cols, starlink_raw_cols):
        """
        Return a list of dict that can be inserted in batch.
        Only gets the columns used by cls
        """
        dict_list_starlink = list()
        dict_list_starlink_raw = list()
        for i, content in df.iterrows():
            row_dict = dict()
            row_raw_dict = dict()
            for col_key in starlink_cols:
                if col_key in content.keys():
                    row_dict[col_key] = content[col_key]
                elif col_key in content['spaceTrack'].keys():
                    row_dict[col_key] = content['spaceTrack'][col_key]
                else:
                    raise ValueError(f"Column '{col_key}' value not found")
            for col_key in starlink_raw_cols:
                row_raw_dict[col_key] = content[col_key]
            row_raw_dict['raw_json'] = dict(content)
            row_dict['EPOCH_UNIX_TIME'] = set_epoch_unix_time(row_dict['EPOCH'])
            dict_list_starlink.append(row_dict)
            dict_list_starlink_raw.append(row_raw_dict)
        return dict_list_starlink, dict_list_starlink_raw



    def run(self, df, batch_size=1000):
        """
        @df is pandas df

        """
        nrows, ncols  = df.shape
        batches_num = math.ceil(nrows / batch_size)
        starlink_cols, starlink_raw_cols = self.get_cls_colums()
        dict_list_starlink_cnt = 0
        dict_list_starlink_raw_cnt = 0
        logging.info(f"Original df size was {df.shape}")
        for batch_idx in range(batches_num):
            r_count = 0
            batch_start = batch_idx * batch_size
            batch_end = (batch_idx+1) * batch_size
            df_batch = pd.DataFrame()
            if batch_end < nrows:
                df_batch = df.iloc[batch_start:batch_end]
            elif batch_end < nrows and batch_start < nrows:
                df_batch = df.iloc[batch_start:batch_end]
            elif batch_end > nrows and batch_start < nrows:
                df_batch = df.iloc[-batch_size:]
            elif batch_end == nrows and batch_start < nrows:
                df_batch = df.iloc[batch_start:batch_end]
            elif batch_start == nrows:
                df_batch = df.iloc[-1]
            elif batch_start > nrows:
                continue
            logging.info(f"BATCH={batch_idx} batch_start={batch_start} batch_end={batch_end} size={df_batch.shape}")
            dict_list_starlink, dict_list_starlink_raw = self.df_rows_to_sql_cls( df_batch, starlink_cols, starlink_raw_cols)
            self._db_engine.execute(
                models.StarLink.__table__.insert(),
                dict_list_starlink
            )
            self._db_engine.execute(
                models.StarLinkRaw.__table__.insert(),
                dict_list_starlink_raw
            )
            dict_list_starlink_cnt += len(dict_list_starlink)
            dict_list_starlink_raw_cnt += len(dict_list_starlink_raw)
        return dict_list_starlink_cnt, dict_list_starlink_raw_cnt
