# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

path = "abfss://imoveis@lakealura.dfs.core.windows.net/inbound (raw)/dados_brutos_imoveis.json"

arquivo_raw = spark.read.json(path)

arquivo_raw.write.mode("overwrite").format("json").save("/Volumes/databricks_curso/default/inbound_raw")

# COMMAND ----------

arquivo_raw = arquivo_raw.drop("usuario", "imagens")

# COMMAND ----------

arquivo_raw = arquivo_raw.withColumn("id", col("anuncio.id"))

# COMMAND ----------

inbound_path_volume = "/Volumes/databricks_curso/default/bronze"
datalake_path = "abfss://imoveis@lakealura.dfs.core.windows.net/bronze (work)/"

arquivo_raw.write.mode("overwrite").format("delta").save(inbound_path_volume)
arquivo_raw.write.mode("overwrite").format("delta").save(datalake_path)
