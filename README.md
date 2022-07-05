# Problem stament

https://github.com/BlueOnionLabs/api-spacex-backend

# Depedencies

* python3
* sqlite3
* requirements.txt


# how use it


* Load JSON file

```
python cli.py load-json-data -f /mnt/c/data-lake/dt_log_2022-07-03/starlink_historical_data.json
```


* Get last 3 events since 2020-10-13

```
PYTHONPATH=. python cli.py get-last-events -s 2020-10-13 -n 3
```


* Get last 3 events between 2020-10-13 and 2020-10-14

```
PYTHONPATH=. python cli.py get-last-events -s 2020-10-13 -e 2020-10-14 -n 3
```

# Run the test

```
PYTHONPATH=. python -m pytest -s -o log_cli=true -o log_cli_level=INFO tests/test_insert_and_query.py
```

# References

* [Hybrid properties in SqlAlchemy](https://docs.sqlalchemy.org/en/14/orm/mapped_attributes.html#using-descriptors-and-hybrids)
* [Datetime and Date types in SQLite](https://www.sqlite.org/datatype3.html)
* [Datetime formats](https://help.sumologic.com/03Send-Data/Sources/04Reference-Information-for-Sources/Timestamps%2C-Time-Zones%2C-Time-Ranges%2C-and-Date-Formats)
    * [strftime() and strptime() Format Codes](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes)
