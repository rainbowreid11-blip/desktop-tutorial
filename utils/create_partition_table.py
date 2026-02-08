#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@File    :   create_partition_table.py
@Time    :   2026/2/7 15:33
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
original_table = client.get_table("data-engineer-pipeline-1.zoomcamp.yellow_tripdata_m")  # get original table info

# define partition rule（base on date）
partition_def = bigquery.TimePartitioning(
    field="tpep_dropoff_datetime",  # original table's time column
    type_=bigquery.TimePartitioningType.DAY
)

clustering_fields = ["VendorID"]
# create new partition table(copy original table schema)
new_table = bigquery.Table(
    "data-engineer-pipeline-1.zoomcamp.yellow_tripdata_m_partitioned",
    schema=original_table.schema
)
new_table.time_partitioning = partition_def
new_table.clustering_fields = clustering_fields
# client.create_table(new_table)
created_table = client.get_table("data-engineer-pipeline-1.zoomcamp.yellow_tripdata_m_partitioned")
print("partition filed：", created_table.time_partitioning.field)
print("clustering filed：", created_table.clustering_fields)
# insert data
query = f"""
    INSERT INTO `data-engineer-pipeline-1.zoomcamp.yellow_tripdata_m_partitioned`
    SELECT * FROM `data-engineer-pipeline-1.zoomcamp.yellow_tripdata_m`
"""
client.query(query).result()  # execute data migration


