from __future__ import annotations

import json
import sys

from pyspark.sql.functions import col

from src.common.config import load_config
from src.common.paths import resolve_input_path, resolve_output_path
from src.common.spark import get_spark
from src.parsing.parse_access_logs import parse_raw_line


def main(env_name: str, process_date: str) -> None:
    config = load_config(env_name)
    spark = get_spark("parse", config)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    raw_input_path = resolve_input_path(config, "raw_text_input")
    normalized_output_path = resolve_output_path(config, "normalized_events")
    quarantine_output_path = resolve_output_path(config, "quarantined_raw_events")

    raw_df = spark.read.text(raw_input_path)

    def parse_wrapper(row):
        return json.dumps(parse_raw_line(row.value, raw_input_path))

    parsed_df = spark.read.json(raw_df.rdd.map(parse_wrapper))

    if process_date:
        parsed_df = parsed_df.filter(col("event.event_date") == process_date)

    normalized_df = parsed_df.filter(col("ok") == True).select("event.*")
    error_df = parsed_df.filter(col("ok") == False).select("error.*")

    normalized_df.write \
     .mode("overwrite") \
     .partitionBy("event_date") \
     .parquet(normalized_output_path)

    error_df.write \
        .mode("overwrite") \
        .parquet(quarantine_output_path)

    print(f"Normalized events written under base path: {normalized_output_path}")
    print(f"Quarantine events written to: {quarantine_output_path}")

    spark.stop()


if __name__ == "__main__":
    env_name = sys.argv[1] if len(sys.argv) > 1 else "local"
    process_date = sys.argv[2] if len(sys.argv) > 2 else None
    main(env_name, process_date)