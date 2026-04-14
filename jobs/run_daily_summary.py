from __future__ import annotations

from pyspark.sql import functions as F

from jobs.cli_utils import parse_args, validate_cli_args, build_process_dates

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

    print(f"Daily abuse summary for event_date={process_date} written to: {output_path}/event_date={process_date}")
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