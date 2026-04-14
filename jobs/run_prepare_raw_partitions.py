from __future__ import annotations

import sys
from src.common.config import load_config
from src.common.paths import resolve_input_path, resolve_output_path
from src.common.spark import get_spark
from src.ingestion.build_raw_partitions import build_raw_partitions


def main(env_name: str) -> None:
    config = load_config(env_name)
    spark = get_spark("prepare-raw-partitions", config)

    source_path = resolve_input_path(config, "raw_text_input")
    raw_partitioned_output_path = resolve_output_path(config, "raw_logs")
    quarantine_output_path = resolve_output_path(config, "quarantined_raw_lines")

    raw_df = spark.read.text(source_path).withColumnRenamed("value", "raw_line")

    valid_df, quarantine_df = build_raw_partitions(raw_df)

    # Phase 2/3 prep job: rebuild from single source file
    valid_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .parquet(raw_partitioned_output_path)

    quarantine_df.coalesce(1).write \
        .mode("overwrite") \
        .parquet(quarantine_output_path)

    print(f"Prepared raw partitions written to: {raw_partitioned_output_path}")
    print(f"Raw partition quarantine written to: {quarantine_output_path}")

    spark.stop()


if __name__ == "__main__":
    env_name = sys.argv[1] if len(sys.argv) > 1 else "local"
    main(env_name)