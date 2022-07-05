#!/usr/bin/python3
from datetime import datetime
import time
import logging

def validate_date_fmt(date_text, date_fmt):
    try:
        datetime.strptime(date_text, date_fmt)
        return True
    except ValueError:
        return False


def set_epoch_unix_time(epoc):
    """
    Receive and EPOCH like:

    "EPOCH": "2020-08-23T03:09:46.298304"
    """
    date_fmt ="%Y-%m-%dT%H:%M:%S.%f"
    date_fmt_opt2 ="%Y-%m-%dT%H:%M:%S"
    if validate_date_fmt(epoc, date_fmt):
        d = datetime.strptime(epoc, date_fmt)
        unixtime = time.mktime(d.timetuple())
    elif validate_date_fmt(epoc, date_fmt_opt2):
        d = datetime.strptime(epoc, date_fmt_opt2)
        unixtime = time.mktime(d.timetuple())
    else:
        logging.error(f"Incorrect date '{epoc}' format. Suported format are: ['{date_fmt}','{date_fmt_opt2}']")
    return unixtime
