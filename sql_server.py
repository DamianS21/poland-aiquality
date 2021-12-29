from config import SQLITE_TABLE_NAME
from sqlalchemy import create_engine
from data_downloader import generate_aggregated_table
import pandas as pd
import time

def open_sql_connection():
    engine = create_engine('sqlite:///airquality.db', echo=True)
    sqlite_connection = engine.connect()
    return engine, sqlite_connection


def close_sql_connection(engine, sqlite_connection):
    sqlite_connection.close()
    engine.dispose()


def import_data_to_db(data_limit=None, sqlite_table_name=SQLITE_TABLE_NAME):
    downloaded_data = generate_aggregated_table(data_limit)
    engine, sqlite_connection = open_sql_connection()

    sqlite_connection.execute(f'''CREATE TABLE IF NOT EXISTS "{sqlite_table_name}" (
    	"date"	TEXT,
    	"value"	NUMERIC,
    	"key"	TEXT,
    	"station_id"	INTEGER,
    	"sensor_id"	INTEGER,
    	"data_key"	TEXT,
    	PRIMARY KEY("data_key")
    );''')

    data_indexes = sqlite_connection.execute(f'SELECT data_key FROM {sqlite_table_name}')
    exs_data_keys = pd.DataFrame(data_indexes.fetchall(), columns=['data_key'])
    outer_join = pd.merge(downloaded_data, exs_data_keys, on=["data_key"], how="outer", indicator=True)
    rows_to_append = outer_join[~(outer_join._merge == 'both')].drop('_merge', axis=1)
    l_rows_to_append = len(rows_to_append)

    print(f'Rows to append: {l_rows_to_append}')

    if l_rows_to_append > 0:
        downloaded_data.to_sql(sqlite_table_name, sqlite_connection, if_exists='replace', index=True)

    close_sql_connection(engine, sqlite_connection)


def generate_csv_from_db(sqlite_table_name=SQLITE_TABLE_NAME):
    engine, sqlite_connection = open_sql_connection()
    df = pd.read_sql(f'''SELECT * FROM {sqlite_table_name}''', sqlite_connection)
    timestr = time.strftime("%Y%m%d-%H%M")
    df.to_csv(f'db_snapshot_{timestr}', index=False)
    close_sql_connection(engine, sqlite_connection)