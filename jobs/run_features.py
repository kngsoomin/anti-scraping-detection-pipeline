from __future__ import annotations

from pyspark.sql import functions as F

from jobs.cli_utils import parse_args, validate_cli_args, build_process_dates

from src.common.config import load_config
from src.common.paths import resolve_output_path
from src.common.spark import get_spark
from src.features.build_session_features import build_session_features


def main(env_name: str, process_date: str) -> None:
    config = load_config(env_name)
    spark = get_spark("features", config)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    input_path = resolve_output_path(config, "sessionized_events")
    output_path = resolve_output_path(config, "session_features")

    sessionized_df = spark.read.parquet(input_path).filter(
            F.col("session_date") == process_date
        )

    session_features_df = build_session_features(sessionized_df)

    session_features_df.write \
        .mode("overwrite") \
        .partitionBy("session_date") \
        .parquet(output_path)

    print(f"Session features for session_date={process_date} written to: {output_path}/session_date={process_date}")
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