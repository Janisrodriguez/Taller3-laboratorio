"""medallion_pipeline.py
Script medallion (Bronze -> Silver -> Gold) con Quality Gate y Quarantine.

Uso (ejemplo):
  python medallion_pipeline.py --input-path /app/data/SECOP_II_Contratos_Electronicos.csv \
    --lakehouse-root /app/data/lakehouse --pipeline-version v1 --mode dev

Características:
 - Ingesta a Bronze (append)
 - Validaciones en Silver (no eliminar inválidos)
 - Registros inválidos -> quarantine with motivo_rechazo, metadata and rejection_code
 - Reprocesamiento de cuarentena (flag --run-reprocess)

Requiere: pyspark, delta-spark
"""

import argparse
import logging
from datetime import date
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    lit,
    when,
    concat_ws,
    expr,
    date_format,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


LOG = logging.getLogger("medallion")

def clean_name(c: str) -> str:
    c = c.strip()
    c = re.sub(r"[;:{}()\n\t\r]", "", c)
    c = re.sub(r"\s+", "_", c)
    c = re.sub(r"[^A-Za-z0-9_]", "", c)
    return c


def build_spark(app_name="medallion_pipeline"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.executor.memory", "1g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def ingest_to_bronze(spark, input_path, bronze_path):
    LOG.info("Leyendo archivo de entrada: %s", input_path)
    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(input_path)
    )

    # Limpiar nombres de columnas (coincidir con el resto del pipeline)
    cols_limpias = [clean_name(c) for c in df_raw.columns]
    for old, new in zip(df_raw.columns, cols_limpias):
        if old != new:
            df_raw = df_raw.withColumnRenamed(old, new)

    # Añadir metadata de ingest
    df_bronze = (
        df_raw
        .withColumn("_ingestion_time", current_timestamp())
        .withColumn("_source_file", lit(input_path))
        .withColumn("_ingestion_date", date_format(current_timestamp(), "yyyy-MM-dd"))
    )

    LOG.info("Escribiendo a Bronze: %s (append, mergeSchema=true)", bronze_path)
    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(bronze_path)
    )
    LOG.info("Ingesta completada. Registros ingestados: %d", df_bronze.count())
    return df_bronze.count()


def apply_quality_checks(df, precio_col, fecha_col):
    # Reglas (según Sección 11):
    # - Precio Base no nulo y > 0
    # - Fecha de Firma no nula y no futura

    precio_not_null = col(precio_col).isNotNull()
    precio_positive = col(precio_col).cast("double") > 0
    fecha_not_null = col(fecha_col).isNotNull()
    fecha_not_future = col(fecha_col) <= current_date()

    # Struct con checks (útil para reporting)
    df_validated = (
        df.withColumn("_quality_checks", expr(
            "struct("
            f"({precio_not_null._jc.toString()}) as precio_not_null, "
            f"({precio_positive._jc.toString()}) as precio_positive, "
            f"({fecha_not_null._jc.toString()}) as fecha_not_null, "
            f"({fecha_not_future._jc.toString()}) as fecha_not_future"
            ")"
        ))
    )

    # _is_valid
    df_validated = df_validated.withColumn(
        "_is_valid",
        col("_quality_checks.precio_not_null")
        & col("_quality_checks.precio_positive")
        & col("_quality_checks.fecha_not_null")
        & col("_quality_checks.fecha_not_future"),
    )

    # Motivo de rechazo (concat de razones)
    df_validated = df_validated.withColumn(
        "_rejection_reasons",
        concat_ws(
            ", ",
            when(~col("_quality_checks.precio_not_null"), lit("Precio Base nulo")),
            when(~col("_quality_checks.precio_positive"), lit("Precio Base <= 0")),
            when(~col("_quality_checks.fecha_not_null"), lit("Fecha de Firma nula")),
            when(~col("_quality_checks.fecha_not_future"), lit("Fecha de Firma futura")),
        ),
    )

    # Rejection code prioritario
    df_validated = df_validated.withColumn(
        "_rejection_code",
        when(~col("_quality_checks.fecha_not_null"), lit("E001"))
        .when(~col("_quality_checks.fecha_not_future"), lit("E002"))
        .when(~col("_quality_checks.precio_not_null"), lit("E003"))
        .when(~col("_quality_checks.precio_positive"), lit("E004"))
        .otherwise(lit(None)),
    )

    return df_validated


def write_clean_and_quarantine(df_validated, silver_path, quarantine_path, pipeline_version="v1", write_mode_clean="merge", write_mode_quarantine="append"):
    # Split
    df_clean = df_validated.filter(col("_is_valid"))
    df_quarantine = (
        df_validated.filter(~col("_is_valid"))
        .withColumn("motivo_rechazo", col("_rejection_reasons"))
        .withColumn("_validation_timestamp", current_timestamp())
        .withColumn("_pipeline_version", lit(pipeline_version))
        .withColumn("_quarantine_id", expr("uuid()"))
        .withColumn("_reprocess_attempts", lit(0))
    )

    LOG.info("Escribiendo %d registros limpios a Silver: %s", df_clean.count(), silver_path)
    (
        df_clean.drop("_quality_checks", "_is_valid", "_rejection_reasons")
        .write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(silver_path)
    )

    LOG.info("Escribiendo %d registros inválidos a Quarantine: %s", df_quarantine.count(), quarantine_path)
    (
        df_quarantine
        .write
        .format("delta")
        .mode(write_mode_quarantine)
        .option("mergeSchema", "true")
        .save(quarantine_path)
    )

    return df_clean.count(), df_quarantine.count()


def reprocess_quarantine(spark, quarantine_path, silver_path, pipeline_version="v1"):
    LOG.info("Reprocesando registros en quarantine: %s", quarantine_path)
    try:
        df_q = spark.read.format("delta").load(quarantine_path)
    except Exception as e:
        LOG.error("No se puede leer quarantine: %s", e)
        return 0

    if df_q.count() == 0:
        LOG.info("No hay registros en quarantine para reprocesar.")
        return 0

    # Re-aplicar validaciones (suponemos mismas columnas)
    cols = df_q.columns
    precio_col = "Precio_Base" if "Precio_Base" in cols else "Precio Base"
    fecha_col = "Fecha_de_Firma" if "Fecha_de_Firma" in cols else "Fecha de Firma"

    df_revalidated = apply_quality_checks(df_q, precio_col, fecha_col)

    df_to_move = df_revalidated.filter(col("_is_valid"))
    moved = df_to_move.count()

    if moved > 0:
        LOG.info("Moviendo %d registros reprocesados a Silver", moved)
        (
            df_to_move.drop("_quality_checks", "_is_valid", "_rejection_reasons")
            .write.format("delta").mode("append").save(silver_path)
        )

        # Actualizar quarantine: incrementar attempts y marcar _reprocessed_at
        try:
            dt = DeltaTable.forPath(spark, quarantine_path)
            spark_sql = (
                "MERGE INTO delta.`{qp}` q "
                "USING (SELECT _quarantine_id as _qid FROM values) s "
                )
            # Usamos condición IN con lista de ids
            ids = [r._quarantine_id for r in df_to_move.select("_quarantine_id").collect()]
            if ids:
                ids_list = ",".join([f"'{i}'" for i in ids])
                update_stmt = (
                    f"MERGE INTO delta.`{quarantine_path}` AS q \n"
                    f"USING (SELECT explode(array({ids_list})) as _quarantine_id) s ON q._quarantine_id = s._quarantine_id \n"
                    f"WHEN MATCHED THEN UPDATE SET q._reprocess_attempts = q._reprocess_attempts + 1, q._reprocessed_at = current_timestamp()"
                )
                spark.sql(update_stmt)
        except Exception as e:
            LOG.warning("No se pudo actualizar attempts en quarantine: %s", e)

    LOG.info("Reprocess completado. Movidos: %d", moved)
    return moved


def notebook_debug_example():
    """Helper para usar desde Jupyter: crea un DataFrame de ejemplo y demuestra
    la limpieza de nombres de columna y la validación sin necesidad de ejecutar
    fragmentos indentados en la celda (evita IndentationError).

    Uso en notebook:
      from medallion_pipeline import notebook_debug_example
      notebook_debug_example()
    """
    spark = build_spark(app_name="medallion_debug")

    # Crear un DataFrame de ejemplo con nombres con espacios y valores inválidos
    data = [
        ("ALCALDIA 1", None, "ANTIOQUIA", None, None, None, None, None, None, "2023-01-01", None, None, 3582652, None, None),
        ("MUNICIPIO 6", None, "NARIÑO", None, None, None, None, None, None, None, None, None, None, None, None),
        ("UNIVERSIDAD 4", None, "VALLE", None, None, None, None, None, None, "2023-04-10", None, None, -100, None, None),
    ]
    cols = [
        "Entidad","Nit Entidad","Departamento","Ciudad","Estado","Descripcion del Proceso",
        "Tipo de Contrato","Modalidad de Contratacion","Justificacion Modalidad de Contratacion",
        "Fecha de Firma","Fecha de Inicio del Contrato","Fecha de Fin del Contrato","Precio Base","Valor Total","Valor Pagado"
    ]

    # Schema explícito para evitar inferencia fallida cuando columnas son todas NULL
    schema = StructType([
        StructField("Entidad", StringType(), True),
        StructField("Nit Entidad", StringType(), True),
        StructField("Departamento", StringType(), True),
        StructField("Ciudad", StringType(), True),
        StructField("Estado", StringType(), True),
        StructField("Descripcion del Proceso", StringType(), True),
        StructField("Tipo de Contrato", StringType(), True),
        StructField("Modalidad de Contratacion", StringType(), True),
        StructField("Justificacion Modalidad de Contratacion", StringType(), True),
        StructField("Fecha de Firma", StringType(), True),
        StructField("Fecha de Inicio del Contrato", StringType(), True),
        StructField("Fecha de Fin del Contrato", StringType(), True),
        StructField("Precio Base", DoubleType(), True),
        StructField("Valor Total", DoubleType(), True),
        StructField("Valor Pagado", DoubleType(), True),
    ])

    df_raw = spark.createDataFrame(data, schema=schema)

    print("Columnas antes:", df_raw.columns)

    # Renombrado usando clean_name (no hay indentación en la celda)
    cols_limpias = [clean_name(c) for c in df_raw.columns]
    for old, new in zip(df_raw.columns, cols_limpias):
        if old != new:
            df_raw = df_raw.withColumnRenamed(old, new)

    print("Columnas después:", df_raw.columns)

    # Aplicar validaciones y mostrar registros inválidos
    precio_col = "Precio_Base" if "Precio_Base" in df_raw.columns else "Precio Base"
    fecha_col = "Fecha_de_Firma" if "Fecha_de_Firma" in df_raw.columns else "Fecha de Firma"
    df_validated = apply_quality_checks(df_raw, precio_col, fecha_col)

    print("Registros inválidos (motivo):")
    df_validated.filter(~col("_is_valid")).select("Entidad", "Precio_Base", "Fecha_de_Firma", "_rejection_code", "_rejection_reasons").show(truncate=False)


def run_pipeline_interactive(
    input_path=None,
    lakehouse_root="data/lakehouse",
    pipeline_version="v1",
    run_reprocess=False,
    mode="dev",
    spark=None,
):
    """Ejecuta el pipeline desde un notebook sin pegar código indentado.

    Retorna un diccionario con métricas básicas y paths.
    """
    local_spark = False
    if spark is None:
        spark = build_spark(app_name="medallion_interactive")
        local_spark = True

    bronze_path = f"{lakehouse_root}/bronze/secop"
    silver_path = f"{lakehouse_root}/silver/secop"
    quarantine_path = f"{lakehouse_root}/quarantine/secop_errors"

    if input_path:
        ingest_to_bronze(spark, input_path, bronze_path)

    try:
        df_bronze = spark.read.format("delta").load(bronze_path)
    except Exception as e:
        LOG.error("No se pudo leer Bronze: %s", e)
        return {"error": str(e)}

    cols = df_bronze.columns
    precio_col = "Precio_Base" if "Precio_Base" in cols else "Precio Base"
    fecha_col = "Fecha_de_Firma" if "Fecha_de_Firma" in cols else "Fecha de Firma"

    df_validated = apply_quality_checks(df_bronze, precio_col, fecha_col)

    clean_count, quarantine_count = write_clean_and_quarantine(
        df_validated, silver_path, quarantine_path, pipeline_version=pipeline_version
    )

    total = df_bronze.count()
    summary = {
        "total": total,
        "clean": clean_count,
        "quarantine": quarantine_count,
        "reject_pct": (100.0 * quarantine_count / total if total > 0 else 0),
    }

    LOG.info("Resumen interactivo: %s", summary)

    if summary["reject_pct"] > 5 and mode == "prod":
        LOG.warning("ALERTA: Tasa de rechazo > 5%% (%.2f%%).", summary["reject_pct"])

    if run_reprocess:
        moved = reprocess_quarantine(spark, quarantine_path, silver_path, pipeline_version)
        summary["moved"] = moved
        LOG.info("Registros reprocesados y movidos: %d", moved)

    if local_spark:
        spark.stop()

    return summary


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Medallion pipeline with quality gate and quarantine")
    p.add_argument("--input-path", required=False, help="Archivo de entrada (CSV)")
    p.add_argument("--lakehouse-root", default="data/lakehouse", help="Ruta raíz del lakehouse")
    p.add_argument("--pipeline-version", default="v1", help="Versión del pipeline")
    p.add_argument("--mode", choices=["dev", "prod"], default="dev")
    p.add_argument("--run-reprocess", action="store_true", help="Ejecutar reprocess sobre quarantine")
    # En entornos como Jupyter el kernel añade args (ej. -f ...); usar parse_known_args evita SystemExit
    if argv is None:
        args, _ = p.parse_known_args()
    else:
        args = p.parse_args(argv)
    return args


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    spark = build_spark()

    bronze_path = f"{args.lakehouse_root}/bronze/secop"
    silver_path = f"{args.lakehouse_root}/silver/secop"
    quarantine_path = f"{args.lakehouse_root}/quarantine/secop_errors"

    if args.input_path:
        ingest_to_bronze(spark, args.input_path, bronze_path)

    # Leer Bronze más reciente
    try:
        df_bronze = spark.read.format("delta").load(bronze_path)
    except Exception as e:
        LOG.error("No se pudo leer Bronze: %s", e)
        return

    cols = df_bronze.columns
    precio_col = "Precio_Base" if "Precio_Base" in cols else "Precio Base"
    fecha_col = "Fecha_de_Firma" if "Fecha_de_Firma" in cols else "Fecha de Firma"

    df_validated = apply_quality_checks(df_bronze, precio_col, fecha_col)

    clean_count, quarantine_count = write_clean_and_quarantine(
        df_validated, silver_path, quarantine_path, pipeline_version=args.pipeline_version
    )

    total = df_bronze.count()
    LOG.info("Resumen: total=%d, validos=%d, rechazados=%d", total, clean_count, quarantine_count)

    # Alerta simple
    reject_pct = 100.0 * quarantine_count / total if total > 0 else 0
    if reject_pct > 5 and args.mode == "prod":
        LOG.warning("ALERTA: Tasa de rechazo > 5%% (%.2f%%).", reject_pct)

    if args.run_reprocess:
        moved = reprocess_quarantine(spark, quarantine_path, silver_path, args.pipeline_version)
        LOG.info("Registros reprocesados y movidos: %d", moved)


if __name__ == "__main__":
    main()
