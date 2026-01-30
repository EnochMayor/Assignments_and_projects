#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# In[2]:


get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet')


# In[2]:


engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5433/ny_taxi')


# In[7]:


df = pd.read_parquet("green_tripdata_2025-11.parquet")


# In[21]:


#df.to_sql(name='green_taxi_data', con=engine, if_exists='replace', index=False)


# In[23]:


print(pd.io.sql.get_schema(df, name='green_taxi_data', con=engine))


# In[24]:


df.info()


# In[3]:


df.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')


# In[4]:


import pyarrow.parquet as pq

# 1. Initialize the Parquet file as a stream
parquet_file = pq.ParquetFile('green_tripdata_2025-11.parquet')

#Iterate through the file in batches (mimics chunksize=100000)
for parquets in tqdm(parquet_file.iter_batches(batch_size=100000), desc="Ingesting batches"):
    # Convert the PyArrow batch to a Pandas DataFrame
    df = parquets.to_pandas()

    # Upload the chunk to PostgreSQL and 'append' so that previous chunks are not deleted!
    df.to_sql(name='green_taxi_data', con=engine, if_exists='append', index=False)

    print(f"Successfully inserted chunk of {len(df)} rows.")


# In[5]:


get_ipython().system('wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv')


# In[6]:


df_zones=pd.read_csv("taxi_zone_lookup.csv")


# In[7]:


df_zones.head()


# In[8]:


df_zones.info()


# In[27]:


# batch_size = 1000
# file_name_zones = 'taxi_zone_lookup.csv'

# df_zones = pd.read_csv(file_name_zones, chunksize=batch_size)

# first_chunk = next(df_zones)


# #Create a table in the Database named "taxi_zone_lookup"
# first_chunk.head(0).to_sql(
#     name="taxi_zone_lookup",
#     con=engine,
#     if_exists="replace",
#     index=False
# )

# #zones = 'taxi_zone_lookup.csv'

# for zone in tqdm(df_zones, desc="Ingesting CSV_data"):

#     zone.to_sql(name='taxi_zone_lookup', con=engine, if_exists="append",
#             index=False)


# In[28]:


df_zones = pd.read_csv("taxi_zone_lookup.csv")

df_zones.to_sql(
    name="taxi_zone_lookup",
    con=engine,
    if_exists="replace",
    index=False
)


# In[ ]:




