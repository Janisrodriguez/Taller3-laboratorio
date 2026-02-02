# %% Test: Ingesta con datos inválidos
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from delta import *
import re

# Configuración Spark en modo local
master_url = "local[*]"

builder = (
    SparkSession.builder
    .appName("Lab_SECOP_TEST")
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

# LECTURA CSV DE PRUEBA
print("Leyendo CSV de prueba con datos inválidos...")
csv_path = "/app/data/test_invalid_data.csv"

df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .load(csv_path)
)

print(f"Registros leídos: {df_raw.count()}")
print(df_raw.show(15))

# LIMPIEZA DE DATOS
def clean_name(c: str) -> str:
    c = c.strip()
    c = re.sub(r"[;:{}()\n\t\r]", "", c)
    c = re.sub(r"\s+", "_", c)
    c = re.sub(r"[^A-Za-z0-9_]", "", c)
    return c

cols_limpias = [clean_name(c) for c in df_raw.columns]

for old, new in zip(df_raw.columns, cols_limpias):
    if old != new:
        df_raw = df_raw.withColumnRenamed(old, new)

# ESCRITURA BRONZE DE PRUEBA
print("Escribiendo en capa Bronze de prueba...")
output_path = "data/lakehouse/bronze/test_secop"

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .save(output_path)
)

print(f"✅ Ingesta completada. Registros: {df_raw.count()}")
