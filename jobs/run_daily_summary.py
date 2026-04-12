from __future__ import annotations

import sys

from pyspark.sql import functions as F

from src.common.config import load_config
from src.common.paths import resolve_output_path
from src.common.spark import get_spark
from src.detection.daily_abuse_summary import build_daily_abuse_summary


def main(env_name: str, process_date: str) -> None:
    config = load_config(env_name)
    spark = get_spark("daily-summary", config)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    input_path = resolve_output_path(config, "suspicious_sessions")
    output_path = resolve_output_path(config, "daily_abuse_summary")
    sample_json_output_path = resolve_output_path(config, "daily_abuse_summary_sample_json")

    suspicious_sessions_df = (
        spark.read.parquet(input_path)
        .filter(F.col("session_date") == process_date)
        .filter(F.col("risk_band") != "benign")
    )

    daily_abuse_summary_df = build_daily_abuse_summary(suspicious_sessions_df)
    daily_abuse_summary_df.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(output_path)

    daily_abuse_summary_df.coalesce(1).write.mode("overwrite").json(sample_json_output_path)

    print(f"Daily abuse summary for event_date={process_date} written to: {output_path}/event_date={process_date}")
    print(f"Sample summary JSON written to: {sample_json_output_path}")
    spark.stop()


if __name__ == "__main__":
    env_name = sys.argv[1] if len(sys.argv) > 1 else "local"
    process_date = sys.argv[2] if len(sys.argv) > 2 else None
    main(env_name, process_date)