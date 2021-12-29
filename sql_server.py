import sqlite3
from sqlalchemy import create_engine
from data_downloader import generate_aggregated_table
import pandas as pd

DATA_LIMIT = None
SQLITE_TABLE_NAME = "stations_sensors_data"

downloaded_data = generate_aggregated_table(DATA_LIMIT)

engine = create_engine('sqlite:///airquality.db', echo=True)
sqlite_connection = engine.connect()

sqlite_connection.execute('''CREATE TABLE IF NOT EXISTS "stations_sensors_data" (
	"date"	TEXT,
	"value"	NUMERIC,
	"key"	TEXT,
	"station_id"	INTEGER,
	"sensor_id"	INTEGER,
	"data_key"	TEXT,
	PRIMARY KEY("data_key")
);''')

data_indexes = sqlite_connection.execute('SELECT data_key FROM stations_sensors_data')

exs_data_keys = pd.DataFrame(data_indexes.fetchall(), columns=['data_key'])
outer_join = pd.merge(downloaded_data, exs_data_keys, on=["data_key"], how="outer", indicator=True)
rows_to_append = outer_join[~(outer_join._merge == 'both')].drop('_merge', axis=1)
print(rows_to_append)
l_rows_to_append = len(rows_to_append)

print(f'Rows to append: {l_rows_to_append}')

if l_rows_to_append > 0:
    downloaded_data.to_sql(SQLITE_TABLE_NAME, sqlite_connection, if_exists='replace', index=True)

sqlite_connection.close()
engine.dispose()