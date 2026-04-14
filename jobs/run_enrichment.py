from __future__ import annotations

from pyspark.sql import functions as F

from jobs.cli_utils import parse_args, validate_cli_args, build_process_dates

from src.common.config import load_config
from src.common.paths import resolve_input_path, resolve_output_path
from src.common.spark import get_spark
from src.enrichment.build_hostname_enrichment import build_hostname_enrichment


def main(env_name: str, process_date: str) -> None:
    config = load_config(env_name)
    spark = get_spark("enrichment", config)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    session_features_input_path = resolve_output_path(config, "session_features")
    enriched_output_path = resolve_output_path(config, "session_features_enriched")
    hostname_csv_path = resolve_input_path(config, "ip_hostname_lookup")

    known_bot_domains = list(config.enrichment["known_bot_domains"])
    known_bot_ua_patterns = list(config.enrichment["known_bot_ua_patterns"])

    session_features_df = (
        spark.read.parquet(session_features_input_path)
        .filter(F.col("session_date") == process_date)
    )

    hostname_df = (
        spark.read.option("header", True)
        .csv(hostname_csv_path)
    )

    enriched_df = build_hostname_enrichment(
        session_features_df=session_features_df,
        hostname_df=hostname_df,
        known_bot_domains=known_bot_domains,
        known_bot_ua_patterns=known_bot_ua_patterns
    )

    enriched_df.write \
        .mode("overwrite") \
        .partitionBy("session_date") \
        .parquet(enriched_output_path)

    print(
        f"Session features enriched for session_date={process_date} written to: "
        f"{enriched_output_path}/session_date={process_date}"
    )

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