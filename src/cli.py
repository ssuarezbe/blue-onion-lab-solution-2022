#!/usr/bin/python3
import click
from pathlib import Path
from data_processing.tasks.save_df_to_sqlite import Pipeline as SaveSqlitePipeline
from data_processing.tasks.last_events_from_sqlite import Pipeline as LastEventsRetrievalPipeline
import pandas as pd

@click.group()
def main():
   pass


@main.command()
@click.pass_context
@click.option('-n', '--num_events',type=int , default=10)
@click.option('-s', '--start_date',type=str, default=None)
@click.option('-e', '--end_date',type=str)
def get_last_events(ctx, start_date, end_date:None, num_events):
    """Get the last @num_events events since Time @date"""
    if end_date is None:
        click.echo(f"Getting last {num_events} since date {start_date}")
    else:
        click.echo(f"Getting last {num_events} from date {start_date} to {end_date}")
    db_name = 'local.db'
    last_events_pipeline = LastEventsRetrievalPipeline(db_name=db_name)
    last_event = last_events_pipeline.run(start_date_str=start_date, end_date_str=end_date, events_cnt=num_events)
    print("Columns: (launch, latitude, longitude, EPOCH, EPOCH_UNIX_TIME)")
    click.echo(f"last_event={last_event}")


@main.command()
@click.pass_context
@click.option('-f','--file', type=click.Path())
@click.option('-c','--clean_db', type=bool, default=True)
def load_json_data(ctx, file, clean_db=True):
    """Add the data from a JSON file to the DB"""
    # clean_db = ctx.obj['clear_db']
    # if clean_db:
    #     click.echo(f"Cleaning DB before inserting rows")
    #clean_db = True
    print(f"Opening file {file}")
    #path = Path(file)

    #if path.stat().st_size == 0:
    #    raise click.UsageError('FILE must not be empty')
    print(f"Reading JSON {file}")
    db_name = 'local.db'
    #_stream = open(path)
    df = pd.read_json(file, orient='records')
    save_sqlite_pipeline = SaveSqlitePipeline(db_name=db_name, clean_db=clean_db)
    last_events_pipeline = LastEventsRetrievalPipeline(db_name=db_name)
    dict_list_starlink_cnt, dict_list_starlink_raw_cnt = save_sqlite_pipeline.run(df=df)
    click.echo(f"Rows inserted {dict_list_starlink_cnt}")


if __name__ == "__main__":
    main()
