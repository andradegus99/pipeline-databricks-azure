# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

path = "abfss://imoveis@lakealura.dfs.core.windows.net/bronze (work)/"

df = spark.read.format("delta").load(path)



# COMMAND ----------

dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")

# COMMAND ----------

dados_detalhados = dados_detalhados.drop("caracteristicas", "endereco")

# COMMAND ----------

inbound_path_volume = "/Volumes/databricks_curso/default/silver"
datalake_path = "abfss://imoveis@lakealura.dfs.core.windows.net/silver (trusted)/"

dados_detalhados.write.mode("overwrite").format("delta").save(datalake_path)
dados_detalhados.write.mode("overwrite").format("delta").save(inbound_path_volume)
