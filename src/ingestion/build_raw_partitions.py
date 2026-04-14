from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def build_raw_partitions(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    base_df = (
        raw_df
        .withColumn(
            "raw_timestamp",
            F.regexp_extract(F.col("raw_line"), r"\[([^\]]+)\]", 1),
        )
        .withColumn("source_file", F.input_file_name())
    )

    missing_ts_df = (
        base_df
        .filter(F.col("raw_timestamp") == "")
        .select(
            "raw_line",
            "raw_timestamp",
            F.lit("timestamp_missing").alias("error_type"),
            F.lit("Could not extract timestamp token from raw line.").alias("error_message"),
            "source_file",
        )
    )

    parsed_df = base_df.withColumn(
        "event_time_parsed",
        F.to_timestamp(F.col("raw_timestamp"), "dd/MMM/yyyy:HH:mm:ss Z"),
    )

    invalid_ts_df = (
        parsed_df
        .filter((F.col("raw_timestamp") != "") & F.col("event_time_parsed").isNull())
        .select(
            "raw_line",
            "raw_timestamp",
            F.lit("timestamp_parse_failed").alias("error_type"),
            F.lit("Failed to parse timestamp using format dd/MMM/yyyy:HH:mm:ss Z.").alias("error_message"),
            "source_file",
        )
    )

    quarantine_df = missing_ts_df.unionByName(invalid_ts_df)

    valid_df = (
        parsed_df
        .filter(F.col("event_time_parsed").isNotNull())
        .withColumn("event_time_utc", F.to_utc_timestamp(F.col("event_time_parsed"), "UTC"))
        .withColumn("dt", F.to_date(F.col("event_time_utc")).cast("string"))
        .select(
            "raw_line",
            "dt",
            "source_file",
        )
    )

    return valid_df, quarantine_df