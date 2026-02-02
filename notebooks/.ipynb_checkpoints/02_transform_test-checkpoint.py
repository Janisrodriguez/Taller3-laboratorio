# %% Test: Transformación con validación de datos inválidos
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from delta import *

spark = (
    configure_spark_with_delta_pip(
        SparkSession.builder
        .appName("Lab_SECOP_TEST_Transform")
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

bronze_path = "data/lakehouse/bronze/test_secop"

print("Leyendo datos de Bronze de prueba...")
df_bronze = spark.read.format("delta").load(bronze_path)

total_bronze = df_bronze.count()
print(f"Total registros en Bronze: {total_bronze}\n")

# Identificar columnas
cols = df_bronze.columns

precio_col = "Precio_Base" if "Precio_Base" in cols else "Precio Base"
fecha_col = "Fecha_de_Firma" if "Fecha_de_Firma" in cols else "Fecha de Firma"

print(f"Usando columna de precio: {precio_col}")
print(f"Usando columna de fecha: {fecha_col}\n")

# Reglas de calidad
cond_precio_ok = (col(precio_col).isNotNull()) & (col(precio_col).cast("double") > 0)
cond_fecha_ok = col(fecha_col).isNotNull()

cond_registro_valido = cond_precio_ok & cond_fecha_ok

# DataFrame de registros válidos
df_validos = df_bronze.filter(cond_registro_valido)

# DataFrame de registros inválidos con motivo_rechazo
df_invalidos = (
    df_bronze
    .filter(~cond_registro_valido)
    .withColumn(
        "motivo_rechazo",
        when(col(fecha_col).isNull(), lit("Fecha de Firma nula"))
        .when(col(precio_col).isNull(), lit("Precio Base nulo"))
        .when(col(precio_col).cast("double") <= 0, lit("Precio Base <= 0"))
        .otherwise(lit("Incumple reglas de calidad"))
    )
)

print(f"✅ Registros válidos (SILVER): {df_validos.count()}")
print(f"❌ Registros inválidos (QUARANTINE): {df_invalidos.count()}\n")

# Mostrar los registros inválidos
if df_invalidos.count() > 0:
    print("=" * 80)
    print("REGISTROS INVÁLIDOS CAPTURADOS:")
    print("=" * 80)
    df_invalidos.select("Entidad", "Precio_Base", "Fecha_de_Firma", "motivo_rechazo").show(20, truncate=False)

# Escritura
silver_path = "data/lakehouse/silver/test_secop"
quarantine_path = "data/lakehouse/quarantine/test_secop_errors"

print(f"\nEscribiendo registros VÁLIDOS en: {silver_path}")
(
    df_validos.write
    .format("delta")
    .mode("overwrite")
    .save(silver_path)
)

print(f"Escribiendo registros INVÁLIDOS en: {quarantine_path}")
(
    df_invalidos.write
    .format("delta")
    .mode("overwrite")
    .save(quarantine_path)
)

print("\n✅ Transformación de prueba completada.")
print(f"   - Silver (válidos): {df_validos.count()} registros")
print(f"   - Quarantine (inválidos): {df_invalidos.count()} registros")
