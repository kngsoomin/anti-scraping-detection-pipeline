from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def build_parse_failure_summary(quarantined_df: DataFrame) -> DataFrame:
    return (
        quarantined_df
        .groupBy("process_date", "error_type")
        .agg(F.count("*").alias("error_count"))
        .orderBy("process_date", "error_type")
    )