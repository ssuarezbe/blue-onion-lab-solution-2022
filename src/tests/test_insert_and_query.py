#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pathlib
from data_processing.tasks.save_df_to_sqlite import Pipeline as SaveSqlitePipeline
from data_processing.tasks.last_events_from_sqlite import Pipeline as LastEventsRetrievalPipeline
from data_processing.models import StarLinkRaw, StarLink
import pandas as pd
import logging
import os
import json

current_path = pathlib.Path(__file__).parent.resolve()
test_data_path = os.path.join(current_path,'test_data.json')
"""
https://stackoverflow.com/questions/4673373/logging-within-pytest-tests

How call it:

PYTHONPATH=. python -m pytest -s -o log_cli=true -o log_cli_level=INFO tests/test_insert_and_query.py
"""
def test_read():
	save_sqlite_pipeline = SaveSqlitePipeline(db_name='local_test.db', clean_db=True)
	logging.info(f"Reading JSON {test_data_path}")
	test_df = pd.read_json(test_data_path, orient='records')
	logging.info(f"test_df={test_df}")
	save_sqlite_pipeline.run(df=test_df)
	assert(test_df.shape == (6,8) and len(test_df.iloc[0]['spaceTrack']) == 41)


def test_df_to_sql_dict():
	save_sqlite_pipeline = SaveSqlitePipeline(db_name='local_test.db', clean_db=True)
	logging.info(f"Reading JSON {test_data_path}")
	test_df = pd.read_json(test_data_path, orient='records')
	starlink_cols, starlink_raw_cols = save_sqlite_pipeline.get_cls_colums()
	dict_list_starlink, dict_list_starlink_raw = save_sqlite_pipeline.df_rows_to_sql_cls(
		df=test_df, starlink_cols=starlink_cols, starlink_raw_cols=starlink_raw_cols)
	assert(len(dict_list_starlink)==6 and len(dict_list_starlink_raw) == 6)


def test_save_to_sqlite():
	save_sqlite_pipeline = SaveSqlitePipeline(db_name='local_test.db', clean_db=True)
	logging.info(f"Reading JSON {test_data_path}")
	test_df = pd.read_json(test_data_path, orient='records')
	dict_list_starlink_cnt, dict_list_starlink_raw_cnt = save_sqlite_pipeline.run(df=test_df)
	assert(dict_list_starlink_cnt==6 and dict_list_starlink_raw_cnt == 6)


def test_save_to_sqlite_batch_2():
	save_sqlite_pipeline = SaveSqlitePipeline(db_name='local_test.db', clean_db=True)
	logging.info(f"Reading JSON {test_data_path}")
	test_df = pd.read_json(test_data_path, orient='records')
	dict_list_starlink_cnt, dict_list_starlink_raw_cnt = save_sqlite_pipeline.run(df=test_df, batch_size=2)
	assert(dict_list_starlink_cnt ==6 and dict_list_starlink_raw_cnt == 6)


def test_query_most_recent():
	save_sqlite_pipeline = SaveSqlitePipeline(db_name='local_test.db', clean_db=True)
	last_events_pipeline = LastEventsRetrievalPipeline(db_name='local_test.db')
	logging.info(f"Reading JSON {test_data_path}")
	test_df = pd.read_json(test_data_path, orient='records')
	dict_list_starlink_cnt, dict_list_starlink_raw_cnt = save_sqlite_pipeline.run(df=test_df)
	last_event = last_events_pipeline.run(start_date_str="2020-10-13")
	assert(len(last_event)== 3)
