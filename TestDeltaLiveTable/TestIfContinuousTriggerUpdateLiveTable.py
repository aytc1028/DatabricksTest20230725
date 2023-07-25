# Databricks notebook source
import dlt
from pyspark.sql.types import *
from pyspark.sql import functions as f

# COMMAND ----------

# Connect to ADLS Gen 2 via Access Key
# spark.conf.set("fs.azure.account.key.test202306262243.dfs.core.windows.net", "5CV92xuFxtfeiaFF7ABE4wpeD0rTVwVKOS1vpgERYdGxkwrz72IgfyeZhbQrwD5K4rDOeKGBCQ6U+AStM4ScOQ==")

# COMMAND ----------

# Connect to ADLS Gen 2 via Service principal
# service_credential = dbutils.secrets.get(scope="<secret-scope>",key="<service-credential-key>")

# spark.conf.set("fs.azure.account.auth.type.test202306262243.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.test202306262243.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.test202306262243.dfs.core.windows.net", "52759f47-18d8-472c-8ab6-3c8a79c1ebe7")
# spark.conf.set("fs.azure.account.oauth2.client.secret.test202306262243.dfs.core.windows.net", "zHv8Q~EcISEOmBVedZs_y91IvalcaoiMWre_Vdwa")
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.test202306262243.dfs.core.windows.net", "https://login.microsoftonline.com/4be2d16a-b370-4742-8d86-001bf8fc9187/oauth2/token")

# COMMAND ----------

src_table = "dummy"
source_path = f"abfss://others@test202306262243.dfs.core.windows.net/{src_table}/"
bronze_path = f"abfss://bronze@test202306262243.dfs.core.windows.net/{src_table}/"
silver_path = f"abfss://silver@test202306262243.dfs.core.windows.net/{src_table}/"

# COMMAND ----------

@dlt.table(
    name=f"bronze_{src_table}",
    path=f"{bronze_path}"
)
def bronze_dummy():
    return (
        spark.read.format("parquet").load(source_path) \
        .withColumn("elt_timestamp", f.current_timestamp()) \
        .withColumn("data_source", f.input_file_name())
    )

# COMMAND ----------

@dlt.table(
    name=f"silver_{src_table}",
    path=f"{silver_path}"
)
def silver_dummy():
    return (
        dlt.read(f"bronze_{src_table}") \
        .withColumn("layer", f.lit("silver")) \
        .withColumn("elt_timestamp",f.current_timestamp())
    )
