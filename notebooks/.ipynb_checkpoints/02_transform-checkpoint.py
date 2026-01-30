# %% 2. Transformación (Capa Plata + Quarantine)
# Aplica reglas de calidad y bifurca los datos:
#   - Registros válidos -> silver/secop
#   - Registros inválidos -> quarantine/secop_errors con motivo_rechazo

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from delta import *

# ==========================
# 1. Configuración de Spark
# ==========================

# Para el taller, usamos Spark en modo local dentro del contenedor
spark = (
    configure_spark_with_delta_pip(
        SparkSession.builder
        .appName("Lab_SECOP_Silver")
        .master("local[*]")   # 👈 IMPORTANTE: modo local, evita el error de master
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

# =================================
# 2. Leer capa Bronze (Delta)
# =================================

bronze_path = "data/lakehouse/bronze/secop"

print("Leyendo datos de Bronze desde:", bronze_path)
df_bronze = spark.read.format("delta").load(bronze_path)

total_bronze = df_bronze.count()
print("Total registros en Bronze:", total_bronze)
print("Columnas disponibles en Bronze:")
print(df_bronze.columns)

# ====================================================
# 3. Identificar columnas de Precio Base y Fecha Firma
#    (aceptamos variantes con y sin guion bajo)
# ====================================================

cols = df_bronze.columns

# Columna de Precio Base
if "Precio_Base" in cols:
    precio_col = "Precio_Base"
elif "Precio Base" in cols:
    precio_col = "Precio Base"
else:
    raise ValueError(
        "No se encontró ninguna columna de Precio Base (busqué 'Precio_Base' y 'Precio Base')."
    )

# Columna de Fecha de Firma
if "Fecha_de_Firma" in cols:
    fecha_col = "Fecha_de_Firma"
elif "Fecha de Firma" in cols:
    fecha_col = "Fecha de Firma"
else:
    raise ValueError(
        "No se encontró ninguna columna de Fecha de Firma (busqué 'Fecha_de_Firma' y 'Fecha de Firma')."
    )

print(f"Usando columna de precio: {precio_col}")
print(f"Usando columna de fecha de firma: {fecha_col}")

# =================================
# 4. Reglas de calidad (Quality Gate)
# =================================
# Regla 1: Precio Base > 0
# Regla 2: Fecha de Firma no nula

cond_precio_ok = col(precio_col).cast("double") > 0
cond_fecha_ok = col(fecha_col).isNotNull()

cond_registro_valido = cond_precio_ok & cond_fecha_ok

# ==============================
# 5. DataFrame de registros válidos (SILVER)
# ==============================

df_validos = df_bronze.filter(cond_registro_valido)

print("Registros válidos (silver):", df_validos.count())

# ==============================
# 6. DataFrame de registros inválidos (QUARANTINE)
# ==============================
# Usamos el filtro inverso ~cond_registro_valido
# y construimos motivo_rechazo con when().otherwise()

df_invalidos = (
    df_bronze
    .filter(~cond_registro_valido)  # 👈 filtro inverso
    .withColumn(
        "motivo_rechazo",
        when(col(precio_col).isNull(), lit("Precio Base nulo"))
        .when(col(precio_col).cast("double") <= 0, lit("Precio Base <= 0"))
        .when(col(fecha_col).isNull(), lit("Fecha de Firma nula"))
        .otherwise(lit("Incumple reglas de calidad"))
    )
)

print("Registros inválidos (quarantine):", df_invalidos.count())

# ==============================
# 7. Escritura en Silver y Quarantine
# ==============================

silver_path = "data/lakehouse/silver/secop"
quarantine_path = "data/lakehouse/quarantine/secop_errors"

print("Escribiendo registros VÁLIDOS en SILVER:", silver_path)
(
    df_validos.write
    .format("delta")
    .mode("overwrite")
    .save(silver_path)
)

print("Escribiendo registros INVÁLIDOS en QUARANTINE:", quarantine_path)
(
    df_invalidos.write
    .format("delta")
    .mode("overwrite")
    .save(quarantine_path)
)

print("✅ Transformación completada.")
print("   - Silver:", df_validos.count(), "registros")
print("   - Quarantine:", df_invalidos.count(), "registros")
