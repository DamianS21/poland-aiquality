import requests
import pandas as pd
from tqdm import tqdm
from config import STATIONS_HTML_, SENSORS_HTML_, DATA_SENSOR_HTML_

STATIONS_HTML = STATIONS_HTML_ + '/findAll'
stations_json = requests.get(STATIONS_HTML).json()
stations_table = pd.json_normalize(stations_json, meta=['city'])
stations_ids = stations_table['id'].unique()


def _get_sensor_list(station_id):
    SENSORS_HTML = SENSORS_HTML_ + f'{station_id}'
    sensors_json = requests.get(SENSORS_HTML).json()
    sensors_table = pd.json_normalize(sensors_json, meta=['param'])
    sensors_ids = sensors_table['id']
    return sensors_ids


def _get_data_for_sensors(sensors_ids, station_id):
    sensors_data_list = []
    for sensor_id in sensors_ids:
        DATA_SENSOR_HTML = DATA_SENSOR_HTML_ + f'{sensor_id}'
        sensor_data_json = requests.get(DATA_SENSOR_HTML).json()
        sensor_data_json['sensor_id'] = int(sensor_id)
        sensor_data_json['station_id'] = int(station_id)
        sensors_data_list.append(sensor_data_json)
    return sensors_data_list


def generate_aggregated_table(limit=None):
    print('Downloading rows for all stations, sensors')
    aggregated_list = []
    for station_id in tqdm(stations_ids[0:limit]):
        sensors_ids = _get_sensor_list(station_id)
        aggregated_list.extend(_get_data_for_sensors(sensors_ids, station_id))

    aggregated_table = pd.json_normalize(aggregated_list, record_path=['values'],
                                         meta=['key', 'station_id', 'sensor_id'])
    aggregated_table['data_key'] = aggregated_table['date'] + '_' + aggregated_table['station_id'].astype(str) + '_' + \
                                   aggregated_table['sensor_id'].astype(str) + '_' + aggregated_table['key']
    aggregated_table = aggregated_table.set_index('data_key')
    aggregated_table = aggregated_table.dropna(axis=0)
    return aggregated_table