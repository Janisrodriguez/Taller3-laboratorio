# Script de Verificación de Datos en Silver y Quarantine
from pyspark.sql import SparkSession
from delta import *

try:
    spark
except:
    spark = (
        configure_spark_with_delta_pip(
            SparkSession.builder
            .appName("Verify_Lab_SECOP")
            .master("local[*]")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.executor.memory", "1g")
        )
        .getOrCreate()
    )

# Leer Silver
silver_path = "data/lakehouse/silver/secop"
df_silver = spark.read.format("delta").load(silver_path)

# Leer Quarantine
quarantine_path = "data/lakehouse/quarantine/secop_errors"
df_quarantine = spark.read.format("delta").load(quarantine_path)

print("\n" + "="*60)
print("📊 REPORTE DE DATOS - SILVER vs QUARANTINE")
print("="*60)

print(f"\n✅ SILVER (Registros Válidos): {df_silver.count()}")
print(f"❌ QUARANTINE (Registros Inválidos): {df_quarantine.count()}")

print("\n" + "-"*60)
print("📋 MUESTRAS DE REGISTROS INVÁLIDOS (Primeros 10)")
print("-"*60)
df_quarantine.select("Entidad", "Precio_Base", "Fecha_de_Firma", "motivo_rechazo").show(10, truncate=False)

print("\n" + "-"*60)
print("📈 RESUMEN DE MOTIVOS DE RECHAZO")
print("-"*60)
df_quarantine.groupBy("motivo_rechazo").count().show(truncate=False)

print("\n" + "="*60)
