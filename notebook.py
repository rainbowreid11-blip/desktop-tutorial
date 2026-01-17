#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd

# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz', nrows=100)


# In[2]:


df.dtypes


# In[3]:


df.shape


# In[5]:


prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)


# In[6]:


get_ipython().system('uv add sqlalchemy psycopg2-binary')


# In[11]:


from sqlalchemy import create_engine
engine=create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[12]:


print(pd.io.sql.get_schema(df,name='yellow_taxi_data',con=engine))


# In[13]:


df.head(n=0).to_sql(name='yellow_taxi_data',con=engine,if_exists='replace')


# In[17]:


df_iter=pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000)




# In[18]:


for df_chunk in df_iter:
    df_chunk.to_sql(name='yellow_taxi_data',con=engine,if_exists='append')
    print("Inserted chunk:",len(df_chunk))


# In[20]:


get_ipython().system('uv add tqdm')
from tqdm.auto import tqdm


# In[21]:


for df_chunk in tqdm(df_iter):
    print(len(df_chunk))

!uv run pgcli -h localhost -p 5432 -u root -d ny_taxi
# In[ ]:




