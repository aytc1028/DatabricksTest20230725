# Databricks notebook source
# Import necessary libraries

import pandas as pd
import random
import datetime
import time

# COMMAND ----------

# MAGIC %pip install fsspec

# COMMAND ----------

# Function (Random data generator)
def rand_data_generator(rec_cnt = 5):
    # define the start and end dates
    start_date = datetime.datetime.now()
    end_date = start_date + datetime.timedelta(seconds=5)    

    data = []
    for i in range(rec_cnt):
        
        # generate a random datetime between the start and end dates
        time_between_dates = end_date - start_date
        random_number_of_seconds = random.randrange(time_between_dates.total_seconds())
        random_date = start_date + datetime.timedelta(seconds=random_number_of_seconds,microseconds=random_number_of_seconds,milliseconds=random_number_of_seconds)

        # generate a random number
        random_value = random.randrange(500)

        data.append([random_date, random_value])
    
    df = pd.DataFrame(data, columns=['ts', 'value'])
    distinct_df = df.groupby(["ts"]).first().reset_index()
    return distinct_df

# COMMAND ----------

# Connect to ADLS Gen 2 via Access Key
spark.conf.set("fs.azure.account.key.test202306262243.dfs.core.windows.net", "5CV92xuFxtfeiaFF7ABE4wpeD0rTVwVKOS1vpgERYdGxkwrz72IgfyeZhbQrwD5K4rDOeKGBCQ6U+AStM4ScOQ==")

# COMMAND ----------

# Connect to ADLS Gen 2 via Service principal
# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.test202306262243.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.test202306262243.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.test202306262243.dfs.core.windows.net", "52759f47-18d8-472c-8ab6-3c8a79c1ebe7")
spark.conf.set("fs.azure.account.oauth2.client.secret.test202306262243.dfs.core.windows.net", "zHv8Q~EcISEOmBVedZs_y91IvalcaoiMWre_Vdwa")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.test202306262243.dfs.core.windows.net", "https://login.microsoftonline.com/4be2d16a-b370-4742-8d86-001bf8fc9187/oauth2/token")

# COMMAND ----------

# List files in path
dbutils.fs.ls("abfss://landing@test202306262243.dfs.core.windows.net/")

# COMMAND ----------

# List files in path
dbutils.fs.ls("abfss://others@test202306262243.dfs.core.windows.net/db")

# COMMAND ----------

for i in range(10):
    df = rand_data_generator()

    current_dt = datetime.datetime.now()
    current_dt_str = current_dt.strftime('%Y%m%d%H%M%S')
    df.to_csv(f"/dbfs/FileStore/tables/{current_dt_str}.csv", index = False)

    dbutils.fs.mv(f"/FileStore/tables/{current_dt_str}.csv", f"abfss://landing@test202306262243.dfs.core.windows.net/table/{current_dt_str}.csv")

    time.sleep(6)

# COMMAND ----------

for i in range(1):
    df = rand_data_generator()

    current_dt = datetime.datetime.now()
    current_dt_str = current_dt.strftime('%Y%m%d%H%M%S')
    df.to_csv(f"/dbfs/FileStore/tables/{current_dt_str}.csv", index = False)

    dbutils.fs.mv(f"/FileStore/tables/{current_dt_str}.csv", f"abfss://others@test202306262243.dfs.core.windows.net/table/{current_dt_str}.csv")

    time.sleep(6)

# COMMAND ----------


