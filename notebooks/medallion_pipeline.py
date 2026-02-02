"""medallion_pipeline.py 
"""

# NOTE: Este archivo es idéntico al medallion_pipeline.py en la raíz.
# Se deja en /app/notebooks para ser ejecutado dentro del contenedor jupyter-lab

import argparse
import logging
from datetime import date

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
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

LOG = logging.getLogger("medallion")


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
    import re

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
    precio_not_null = col(precio_col).isNotNull()
    precio_positive = col(precio_col).cast("double") > 0
    fecha_not_null = col(fecha_col).isNotNull()
    fecha_not_future = col(fecha_col) <= current_date()

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

    df_validated = df_validated.withColumn(
        "_is_valid",
        col("_quality_checks.precio_not_null")
        & col("_quality_checks.precio_positive")
        & col("_quality_checks.fecha_not_null")
        & col("_quality_checks.fecha_not_future"),
    )

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

        try:
            dt = DeltaTable.forPath(spark, quarantine_path)
            spark_sql = (
                "MERGE INTO delta.`{qp}` q "
                "USING (SELECT _quarantine_id as _qid FROM values) s "
                )
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


def parse_args():
    p = argparse.ArgumentParser(description="Medallion pipeline with quality gate and quarantine")
    p.add_argument("--input-path", required=False, help="Archivo de entrada (CSV)")
    p.add_argument("--lakehouse-root", default="data/lakehouse", help="Ruta raíz del lakehouse")
    p.add_argument("--pipeline-version", default="v1", help="Versión del pipeline")
    p.add_argument("--mode", choices=["dev", "prod"], default="dev")
    p.add_argument("--run-reprocess", action="store_true", help="Ejecutar reprocess sobre quarantine")
    return p.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()

    spark = build_spark()

    bronze_path = f"{args.lakehouse_root}/bronze/secop"
    silver_path = f"{args.lakehouse_root}/silver/secop"
    quarantine_path = f"{args.lakehouse_root}/quarantine/secop_errors"

    if args.input_path:
        ingest_to_bronze(spark, args.input_path, bronze_path)

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

    reject_pct = 100.0 * quarantine_count / total if total > 0 else 0
    if reject_pct > 5 and args.mode == "prod":
        LOG.warning("ALERTA: Tasa de rechazo > 5%% (%.2f%%).", reject_pct)

    if args.run_reprocess:
        moved = reprocess_quarantine(spark, quarantine_path, silver_path, args.pipeline_version)
        LOG.info("Registros reprocesados y movidos: %d", moved)


if __name__ == "__main__":
    main()
