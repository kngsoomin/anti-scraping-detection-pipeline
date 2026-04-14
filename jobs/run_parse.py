from __future__ import annotations

import json

from pyspark.sql import functions as F
from pyspark.sql.functions import col

from jobs.cli_utils import parse_args, validate_cli_args, build_process_dates

from src.common.config import load_config
from src.common.paths import resolve_input_path, resolve_output_path
from src.common.spark import get_spark
from src.parsing.parse_access_logs import parse_raw_line


def main(env_name: str, process_date: str) -> None:
    config = load_config(env_name)
    spark = get_spark("parse", config)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    raw_partition_base_path = resolve_output_path(config, "raw_logs")
    raw_input_path = f"{raw_partition_base_path}/dt={process_date}"

    normalized_output_path = resolve_output_path(config, "normalized_events")
    quarantine_output_path = resolve_output_path(config, "quarantined_raw_events")

    raw_df = (
        spark.read.parquet(raw_input_path)
        .select("raw_line", "source_file")
    )

    def parse_wrapper(row):
        return json.dumps(
            parse_raw_line(
                raw_line=row["raw_line"],
                source_file=row["source_file"],
            )
        )

    parsed_df = spark.read.json(raw_df.rdd.map(parse_wrapper))

    if process_date:
        parsed_df = parsed_df.filter(col("event.event_date") == process_date)

    normalized_df = parsed_df.filter(col("ok") == True).select("event.*")
    error_df = (
        parsed_df.filter(col("ok") == False)
        .select("error.*")
        .withColumn("process_date", F.lit(process_date))
    )

    normalized_df.write \
     .mode("overwrite") \
     .partitionBy("event_date") \
     .parquet(normalized_output_path)

    error_df.write \
        .mode("overwrite") \
        .partitionBy("process_date") \
        .parquet(quarantine_output_path)

    print(
        f"Normalized events for event_date={process_date} written to: "
        f"{normalized_output_path}/event_date={process_date}"
    )
    print(f"Parse quarantine appended to: {quarantine_output_path}")

    spark.stop()

if __name__ == "__main__":
    args = parse_args()
    validate_cli_args(args)
    process_dates = build_process_dates(
        process_date=args.process_date,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    if len(process_dates) != 1:
        raise ValueError("Exactly one date must be provided. Use --process-date to test a single job")

    main(args.env_name, process_dates[0])