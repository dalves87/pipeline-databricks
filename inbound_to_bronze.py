# Databricks notebook source
display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/datalake')

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/datalake/inbound')

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/datalake/bronze')

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/datalake/silver')

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/datalake'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/datalake/inbound'))

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/tables/datalake/inbound/dados_brutos_imoveis.json")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

type(df)

# COMMAND ----------

df = df.drop("imagens","usuario")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

df_bronze = df.withColumn("id", col("anuncio.id"))
display(df_bronze)

# COMMAND ----------

df_bronze.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/datalake/bronze/dataset_imoveis")

# COMMAND ----------


