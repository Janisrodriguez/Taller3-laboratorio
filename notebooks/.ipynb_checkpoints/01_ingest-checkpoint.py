# %% [markdown]
# # 1. Ingesta (Capa Bronce)
# Convertimos CSV crudo a formato Delta Lake.

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from delta import *
import re

# Configuración Spark en modo local
master_url = "local[*]"

builder = (
    SparkSession.builder
    .appName("Lab_SECOP_Bronze")
    .master(master_url)
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.executor.memory", "1g")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# LECTURA CSV
print("Leyendo CSV crudo...")
csv_path = "/app/data/SECOP_II_Contratos_Electronicos.csv"
print("Usando archivo:", csv_path)

df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .load(csv_path)
)

print("Columnas originales:")
print(df_raw.columns)

import re

print("Columnas originales:")
print(df_raw.columns)

# LIMPIEZA DE DATOS
def clean_name(c: str) -> str:
    # quitar espacios al inicio/fin y caracteres invisibles
    c = c.strip()
    # eliminar caracteres problemáticos específicos
    c = re.sub(r"[;:{}()\n\t\r]", "", c)
    # reemplazar uno o más espacios por guion bajo
    c = re.sub(r"\s+", "_", c)
    # por si queda algo raro, deja solo letras, números y _
    c = re.sub(r"[^A-Za-z0-9_]", "", c)
    return c

cols_limpias = [clean_name(c) for c in df_raw.columns]

for old, new in zip(df_raw.columns, cols_limpias):
    if old != new:
        df_raw = df_raw.withColumnRenamed(old, new)

print("Columnas limpias:")
print(df_raw.columns)

# ESCRITURA BRONZE (Delta)
print("Escribiendo en capa Bronze...")
output_path = "data/lakehouse/bronze/secop"

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .save(output_path)
)

print(f"Ingesta completada. Registros procesados: {df_raw.count()}")


