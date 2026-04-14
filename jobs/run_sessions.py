from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import functions as F

from jobs.cli_utils import parse_args, validate_cli_args, build_process_dates

from src.common.config import load_config
from src.common.paths import resolve_output_path
from src.common.spark import get_spark
from src.sessionization.build_sessions import build_sessionized_events


def get_prev_date(process_date: str) -> str:
    dt = datetime.strptime(process_date, "%Y-%m-%d").date()
    return (dt - timedelta(days=1)).isoformat()


def get_lookback_start_ts(process_date: str, session_timeout_seconds: int) -> str:
    process_dt = datetime.strptime(process_date, "%Y-%m-%d")
    lookback_start = process_dt - timedelta(seconds=session_timeout_seconds)
    return lookback_start.strftime("%Y-%m-%d %H:%M:%S")


def main(env_name: str, process_date: str) -> None:
    config = load_config(env_name)
    spark = get_spark("sessions", config)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    input_path = resolve_output_path(config, "normalized_events")
    output_path = resolve_output_path(config, "sessionized_events")

    session_timeout_seconds = int(config.sessionization["timeout_seconds"])
    sample_mode = bool(config.runtime["sample_mode"])
    sample_row_limit = config.runtime["sample_row_limit"]
    prev_date = get_prev_date(process_date)
    lookback_start_ts = get_lookback_start_ts(process_date, session_timeout_seconds)

    normalized_df = spark.read.parquet(input_path)

    # prev_date (D-1): get last timeout window for session boundary
    prev_df = (
        normalized_df
        .filter(F.col("event_date") == prev_date)
        .withColumn("event_time_ts", F.to_timestamp("event_time"))
        .filter(F.col("event_time_ts") >= F.to_timestamp(F.lit(lookback_start_ts)))
        .drop("event_time_ts")
    )
    # process_date (D)
    curr_df = normalized_df.filter(F.col("event_date") == process_date)

    if sample_mode and sample_row_limit:
        curr_df = curr_df.limit(int(sample_row_limit))

    filtered_df = prev_df.unionByName(curr_df)
    filtered_df.groupBy("event_date").count().orderBy("event_date").show(truncate=False)

    sessionized_df = build_sessionized_events(
        normalized_df=filtered_df,
        session_timeout_seconds=session_timeout_seconds,
    )

    output_df = sessionized_df.filter(
        F.col("session_date") == process_date
    )

    output_df = output_df.coalesce(10)

    output_df.write \
        .mode("overwrite") \
        .partitionBy("session_date") \
        .parquet(output_path)

    print(f"Sessionized events for event_date={process_date} written to: {output_path}/session_date={process_date}")
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