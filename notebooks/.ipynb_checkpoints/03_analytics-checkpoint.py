# %% [markdown]
# # 3. Analítica (Capa Oro)
# Agregaciones de negocio.

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc, col
from delta import *

# Builder: puedes usar local[*] o spark://spark-master:7077 si tu cluster está ok
builder = (
    SparkSession.builder
    .appName("Lab_SECOP_Gold")
    .master("local[*]")  # <-- si quieres usar el cluster: "spark://spark-master:7077"
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# %%
# Leer Plata
df_silver = spark.read.format("delta").load("data/lakehouse/silver/secop")

print("Columnas en Silver:", df_silver.columns)
print("Registros en Silver:", df_silver.count())

# %%
# Agregación (Shuffle)
# Usamos los nombres reales: Departamento y Precio_Base
df_gold = (
    df_silver
    .groupBy("Departamento")
    .agg(sum(col("Precio_Base").cast("double")).alias("total_contratado"))
    .orderBy(desc("total_contratado"))
    .limit(10)
)

# %%
# Persistir Oro
df_gold.write.format("delta").mode("overwrite").save("data/lakehouse/gold/top_deptos")

# %%
# Visualizar
print("Top 10 Departamentos por contratación:")
df_pandas = df_gold.toPandas()
print(df_pandas)
