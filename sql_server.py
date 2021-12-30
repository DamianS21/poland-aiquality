from config import BQ_TABLE_ID
from data_downloader import generate_aggregated_table
import pandas as pd
from google.cloud import bigquery


def import_data_to_db(data_limit=None, table_id=BQ_TABLE_ID):
    #if not data_limit:
    #    data_limit = None
    #if not table_id:
    #    table_id = BQ_TABLE_ID
    downloaded_data = generate_aggregated_table(limit=data_limit)
    bqclient = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    query_string = f'SELECT data_key FROM {BQ_TABLE_ID}'
    data_indexes = bqclient.query(query_string).result().to_dataframe()
    print(data_indexes)
    exs_data_keys = pd.DataFrame(data_indexes, columns=['data_key'])
    outer_join = pd.merge(downloaded_data, exs_data_keys, on=["data_key"], how="outer", indicator=True)
    rows_to_append = outer_join[~(outer_join._merge == 'both')].drop('_merge', axis=1)
    l_rows_to_append = len(rows_to_append)

    print(f'Rows to append: {l_rows_to_append}')

    if l_rows_to_append > 0:
        try:
            job = bqclient.load_table_from_dataframe(rows_to_append, table_id, job_config=job_config)
            print(job.result())
        except:
            print(job.exception())

if __name__ == '__main__':
    import_data_to_db(data_limit=None, table_id=BQ_TABLE_ID)