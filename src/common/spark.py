from __future__ import annotations

from pyspark.sql import SparkSession

from src.common.config import AppConfig


def get_spark(app_name_suffix: str, config: AppConfig) -> SparkSession:
    spark_cfg = config.spark
    app_name = f"{spark_cfg['app_name_prefix']}-{app_name_suffix}"

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_cfg["master"])
        .config(
            "spark.sql.parquet.enableVectorizedReader",
            str(spark_cfg["parquet_vectorized_reader"]).lower(),
        )
        .config(
            "spark.sql.files.maxPartitionBytes",
            str(spark_cfg["max_partition_bytes"]),
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(spark_cfg["shuffle_partitions"]),
        )
    )

    return builder.getOrCreate()