# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in test20230724dlt

# COMMAND ----------

spark.conf.set("fs.azure.account.key.test202306262243.dfs.core.windows.net", "5CV92xuFxtfeiaFF7ABE4wpeD0rTVwVKOS1vpgERYdGxkwrz72IgfyeZhbQrwD5K4rDOeKGBCQ6U+AStM4ScOQ==")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test20230724dlt.bronze_dummy

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test20230724dlt.silver_dummy

# COMMAND ----------


