# Databricks notebook source
# Carga CSV 
df_raw = spark.read.table("autos_vendidos_chile")

# Muestra el contenido del DataFrame para verificar que se cargó correctamente
df_raw.show()

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DoubleType

# Limpia y convierte la columna 'valor' a tipo Double
df_processed = df_raw.withColumn("valor", regexp_replace(col("valor"), "[$,]", "").cast(DoubleType()))

# Muestra el esquema y los datos procesados
df_processed.printSchema()
df_processed.show()

# COMMAND ----------

# Nombre tabla
table_name = "autos_procesados_process"

# Define la base de datos donde se va guardar la tabla. Si no existe, la crea.
database_name = "autos_databricks"

# Crear la base de datos si no existe
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# Guarda el DataFrame procesado como una tabla Delta en el Catálogo de Unity
df_processed.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.{table_name}")

print(f"Los datos procesados se han guardado como una tabla en el Catálogo de Unity: {database_name}.{table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM autos_databricks.autos_procesados_process where modelo like '%M%';