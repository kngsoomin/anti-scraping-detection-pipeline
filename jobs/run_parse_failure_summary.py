from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.errors import AnalysisException

from jobs.cli_utils import parse_args, validate_cli_args, build_process_dates

from src.common.config import load_config
from src.common.paths import resolve_output_path
from src.common.spark import get_spark
from src.monitoring.parse_failure_summary import build_parse_failure_summary


def main(env_name: str, process_date: str) -> None:
    if not process_date:
        raise ValueError("process_date is required")

    config = load_config(env_name)
    spark = get_spark("parse-failure-summary", config)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    quarantine_path = resolve_output_path(config, "quarantined_raw_events")
    output_path = resolve_output_path(config, "parse_failure_summary")

    try:
        quarantined_df = spark.read.parquet(quarantine_path)
    except AnalysisException:
        print(
            f"No quarantined_raw_events dataset found. "
            f"Skipping parse failure summary for process_date={process_date}."
        )
        spark.stop()
        return

    process_date_df = quarantined_df.filter(F.col("process_date") == process_date)

    if process_date_df.limit(1).count() == 0:
        print(
            f"No parse failures found for process_date={process_date}. "
            f"Skipping parse failure summary."
        )
        spark.stop()
        return

    summary_df = build_parse_failure_summary(process_date_df)

    summary_df.write \
        .mode("overwrite") \
        .partitionBy("process_date") \
        .parquet(output_path)

    print(f"Parse failure summary written to: {output_path}/process_date={process_date}")

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