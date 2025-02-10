# Databricks notebook source
dbutils.fs.ls("/FileStore/tables/datalake/bronze")

# COMMAND ----------

df = spark.read.format("delta").load("dbfs:/FileStore/tables/datalake/bronze/dataset_imoveis")
display(df)

# COMMAND ----------

display(df.select("anuncio.*"))

# COMMAND ----------

df_detalhado = df.select("anuncio.*", "anuncio.endereco.*")

# COMMAND ----------

display(df_detalhado)

# COMMAND ----------

df_silver = df_detalhado.drop("caracteristicas", "endereco")
display(df_silver)

# COMMAND ----------

df_silver.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/datalake/silver/dataset_imoveis")
