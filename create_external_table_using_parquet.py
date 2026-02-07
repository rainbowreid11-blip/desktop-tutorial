#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@File    :   create_external_table_using_parquet.py
@Time    :   2026/2/7 13:25
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


def create_table_from_gcs_parquet(project_id, dataset_id, table_id, gcs_uris):
    client = bigquery.Client(
        project=project_id,
        credentials=credentials
    )
    table_ref = client.dataset(dataset_id).table(table_id)

    # configuration external table parameter
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = gcs_uris  # GCS file folder list

    # create table
    table = bigquery.Table(table_ref)
    table.external_data_configuration = external_config
    table = client.create_table(table)  # execute create
    print(f"Success create table：{table.project}.{table.dataset_id}.{table.table_id}")


# example
create_table_from_gcs_parquet(
    project_id="data-engineer-pipeline-1",
    dataset_id="zoomcamp",
    table_id="yellow_tripdata",
    gcs_uris=["gs://this_is_jane_unique_gcp_bucket_name/*.parquet"]  # 支持通配符
)




