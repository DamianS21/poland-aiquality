import json
import requests
import pandas as pd
from config import STATIONS_HTML_, SENSORS_HTML_, DATA_SENSOR_HTML_
from pandas.io.json import json_normalize

STATIONS_HTML = STATIONS_HTML_ + '/findAll'

stations_json = requests.get(STATIONS_HTML).json()
stations_table = pd.json_normalize(stations_json, meta = ['city'])

# Get sensors ID
station_id = input()

SENSORS_HTML = SENSORS_HTML_+f'{station_id}'

sensors_json = requests.get(SENSORS_HTML).json()
sensors_table = pd.json_normalize(sensors_json, meta = ['param'])

sensors_ids = sensors_table['id']


for sensor_id in sensors_ids:
  DATA_SENSOR_HTML = DATA_SENSOR_HTML_+ f'{sensor_id}'
  sensor_data_json = requests.get(DATA_SENSOR_HTML).json()
  sensor_data_table = pd.json_normalize(sensor_data_json, record_path  = ['values'], meta = ['key'])
  print(sensor_data_table)