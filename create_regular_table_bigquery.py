#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@File    :   create_regular_table_bigquery.py
@Time    :   2026/2/7 14:03
@Author  :   JianQian   
@Version :   1.0
@Contact :   rainbowreid11@gmail.com  
"""
from google.cloud import bigquery
from google.oauth2 import service_account
CREDENTIALS_FILE = r"F:\Download\data\data-engineer-pipeline-1-33747ba559c1.json"
credentials = service_account.Credentials.from_service_account_file(
    CREDENTIALS_FILE
)
# initialize BigQuery client
client = bigquery.Client(
    project="data-engineer-pipeline-1",
    credentials=credentials
)
# configuration create task
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,  #
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite exist table
)

# define GCS file folder and target
uri = "gs://this_is_jane_unique_gcp_bucket_name/*.parquet"
table_id = "data-engineer-pipeline-1.zoomcamp.yellow_tripdata_m"

# execution load
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)
load_job.result()  # wait task finished

# verify table if created
table = client.get_table(table_id)
print(f"Success create table，include {table.num_rows} row data")
