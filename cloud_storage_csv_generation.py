from google.cloud import storage
from google.cloud import bigquery
from config import GCS_BUCKET, BQ_TABLE_ID, BLOB_PATH
import pandas as pd
import os
import tempfile
from werkzeug.utils import secure_filename


def get_file_path(filename):
    file_name = secure_filename(filename)
    return os.path.join(tempfile.gettempdir(), file_name)


def generate_csv(filename):
    bqclient = bigquery.Client()
    query_string = f'SELECT data_key,date,key,station_id,sensor_id,value FROM {BQ_TABLE_ID}'
    table_download = bqclient.query(query_string).result().to_dataframe()
    big_table = pd.DataFrame(table_download, columns=['data_key','date','key','station_id','sensor_id','value'])
    path_name = get_file_path(filename)
    big_table.to_csv(path_name, index=False)
    #upload_to_gcs_bucket(path_name+'.csv', GCS_BUCKET, blob_path=BLOB_PATH)
    #os.remove(path_name)


def upload_to_gcs_bucket(file_name_to_upload, gcs_bucket=GCS_BUCKET, blob_path=BLOB_PATH):
    client = storage.Client()
    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(file_name_to_upload)
    return blob.url

    generate_csv('db_snapshot')