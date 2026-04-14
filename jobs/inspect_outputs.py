from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.cli_utils import parse_args, validate_cli_args, build_process_dates

from src.common.config import load_config
from src.common.paths import resolve_output_path
from src.common.spark import get_spark


def print_section(title: str) -> None:
    print(f"\n{'=' * 20} {title} {'=' * 20}")


def apply_date_filter(df: DataFrame, process_date: str | None) -> DataFrame:
    if not process_date:
        return df

    if "session_date" in df.columns:
        return df.filter(F.col("session_date") == process_date)

    if "event_date" in df.columns:
        return df.filter(F.col("event_date") == process_date)

    return df


def safe_read_parquet(spark, path: str) -> DataFrame | None:
    try:
        return spark.read.parquet(path)
    except Exception as exc:
        print(f"[WARN] Could not read {path}: {exc}")
        return None


def show_row_counts(spark, paths: dict[str, str], process_date: str | None) -> None:
    print_section("ROW COUNTS")

    for name, path in paths.items():
        df = safe_read_parquet(spark, path)
        if df is None:
            print(f"{name}: None")
            continue

        df = apply_date_filter(df, process_date)
        print(f"{name}: {df.count()}")


def show_quarantine_stats(spark, quarantine_path: str) -> None:
    print_section("QUARANTINE STATS")

    df = safe_read_parquet(spark, quarantine_path)
    if df is None:
        return

    print(f"quarantined_raw_events row count: {df.count()}")

    if "error_type" in df.columns:
        (
            df.groupBy("error_type")
            .count()
            .orderBy(F.desc("count"))
            .show(20, truncate=False)
        )


def show_risk_distribution(spark, suspicious_path: str, process_date: str | None) -> None:
    print_section("RISK BAND DISTRIBUTION")

    df = safe_read_parquet(spark, suspicious_path)
    if df is None:
        return

    df = apply_date_filter(df, process_date)

    (
        df.groupBy("risk_band")
        .count()
        .orderBy(
            F.when(F.col("risk_band") == "high", 1)
             .when(F.col("risk_band") == "medium", 2)
             .when(F.col("risk_band") == "low", 3)
             .when(F.col("risk_band") == "benign", 4)
             .otherwise(5)
        )
        .show(truncate=False)
    )


def show_suspicious_samples(
    spark,
    suspicious_path: str,
    process_date: str | None,
    limit: int = 10,
    exclude_benign: bool = True,
) -> None:
    print_section("SUSPICIOUS SESSION SAMPLES")

    df = safe_read_parquet(spark, suspicious_path)
    if df is None:
        return

    df = apply_date_filter(df, process_date)

    if exclude_benign:
        df = df.filter(F.col("risk_band") != "benign")

    preferred_cols: list[str] = [
        "session_id",
        "session_date",
        "src_ip",
        "user_agent",
        "session_start_ts",
        "request_count",
        "requests_per_minute",
        "session_duration_sec",
        "unique_paths",
        "unique_path_templates",
        "missing_referer_ratio",
        "html_to_asset_ratio",
        "status_4xx_ratio",
        "mean_inter_request_gap",
        "hostname",
        "has_hostname",
        "is_known_bot_domain",
        "context_tag",
        "rule_score",
        "risk_band",
        "top_reasons",
    ]
    existing_cols = [c for c in preferred_cols if c in df.columns]

    (
        df.orderBy(F.desc("rule_score"), F.desc("requests_per_minute"))
        .select(*existing_cols)
        .show(limit, truncate=False)
    )


def show_daily_summary_samples(
    spark,
    daily_summary_path: str,
    process_date: str | None,
    limit: int = 10,
) -> None:
    print_section("DAILY ABUSE SUMMARY SAMPLE")

    df = safe_read_parquet(spark, daily_summary_path)
    if df is None:
        return

    df = apply_date_filter(df, process_date)
    df.orderBy("event_date").show(limit, truncate=False)


def show_feature_grain_check(
    spark,
    session_features_path: str,
    process_date: str | None,
    title: str,
) -> None:
    print_section(title)

    df = safe_read_parquet(spark, session_features_path)
    if df is None:
        return

    df = apply_date_filter(df, process_date)

    total_rows = df.count()
    distinct_sessions = df.select("session_id").distinct().count() if "session_id" in df.columns else None

    print(f"rows: {total_rows}")
    print(f"distinct session_id: {distinct_sessions}")
    print(f"grain_ok: {total_rows == distinct_sessions}")


def show_context_distribution(spark, suspicious_path: str, process_date: str | None) -> None:
    print_section("CONTEXT TAG DISTRIBUTION")

    df = safe_read_parquet(spark, suspicious_path)
    if df is None:
        return

    df = apply_date_filter(df, process_date)

    if "context_tag" not in df.columns:
        print("[INFO] context_tag column not found.")
        return

    (
        df.groupBy("context_tag")
        .count()
        .orderBy(F.desc("count"))
        .show(truncate=False)
    )


def main(env_name: str, process_date: str | None = None) -> None:
    config = load_config(env_name)
    spark = get_spark("inspect-outputs", config)

    paths = {
        "normalized_events": resolve_output_path(config, "normalized_events"),
        "quarantined_raw_events": resolve_output_path(config, "quarantined_raw_events"),
        "sessionized_events": resolve_output_path(config, "sessionized_events"),
        "session_features": resolve_output_path(config, "session_features"),
        "session_features_enriched": resolve_output_path(config, "session_features_enriched"),
        "suspicious_sessions": resolve_output_path(config, "suspicious_sessions"),
        "daily_abuse_summary": resolve_output_path(config, "daily_abuse_summary"),
    }

    if process_date:
        print(f"\nInspecting outputs for process_date={process_date}")

    show_row_counts(spark, paths, process_date)
    show_quarantine_stats(spark, paths["quarantined_raw_events"])
    show_feature_grain_check(
        spark, paths["session_features"], process_date, "SESSION FEATURE GRAIN CHECK"
    )
    show_feature_grain_check(
        spark, paths["session_features_enriched"], process_date, "ENRICHED SESSION FEATURE GRAIN CHECK"
    )
    show_risk_distribution(spark, paths["suspicious_sessions"], process_date)
    show_suspicious_samples(
        spark,
        paths["suspicious_sessions"],
        process_date,
        limit=10,
        exclude_benign=True,
    )
    show_context_distribution(spark, paths["suspicious_sessions"], process_date)
    show_daily_summary_samples(spark, paths["daily_abuse_summary"], process_date, limit=10)

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
        raise ValueError(
            "Exactly one date must be provided. Use --process-date to test a single job."
        )

    main(args.env_name, process_dates[0])