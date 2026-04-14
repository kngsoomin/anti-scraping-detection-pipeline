from __future__ import annotations

from pyspark.sql import SparkSession, functions as F
from pyspark.errors import AnalysisException

from src.common.config import load_config
from src.common.paths import resolve_output_path


def count_partition_rows(
    spark: SparkSession,
    base_path: str,
    partition_col: str,
    process_date: str,
) -> int:
    partition_path = f"{base_path}/{partition_col}={process_date}"

    return spark.read.parquet(partition_path).count()


def count_filtered_partition_rows(
    spark: SparkSession,
    base_path: str,
    partition_col: str,
    process_date: str,
    extra_filter,
) -> int:
    partition_path = f"{base_path}/{partition_col}={process_date}"

    return (
        spark.read.parquet(partition_path)
        .filter(extra_filter)
        .count()
    )


def count_duplicate_rows(
    spark: SparkSession,
    path: str,
    partition_col: str,
    process_date: str,
    key_col: str,
) -> int:
    df = spark.read.parquet(f"{path}/{partition_col}={process_date}")
    total_rows = df.count()
    distinct_rows = df.select(key_col).distinct().count()
    return total_rows - distinct_rows


def count_nulls(
    spark: SparkSession,
    path: str,
    partition_col: str,
    process_date: str,
    col_name: str,
) -> int:
    df = spark.read.parquet(f"{path}/{partition_col}={process_date}")
    return df.filter(F.col(col_name).isNull()).count()


def count_out_of_range(
    spark,
    path,
    partition_col,
    process_date,
    col_name,
    lower,
    upper=None,
):
    df = spark.read.parquet(f"{path}/{partition_col}={process_date}")

    cond = F.col(col_name) < lower
    if upper is not None:
        cond = cond | (F.col(col_name) > upper)

    return df.filter(cond).count()


def count_all_rows(spark: SparkSession, path: str) -> int:
    return spark.read.parquet(path).count()


def count_invalid_values(
    spark: SparkSession,
    path: str,
    partition_col: str,
    process_date: str,
    col_name: str,
    valid_values: list[str],
) -> int:
    df = spark.read.parquet(f"{path}/{partition_col}={process_date}")
    return df.filter(~F.col(col_name).isin(valid_values)).count()


def safe_count_all_rows(spark: SparkSession, path: str) -> int:
    try:
        return spark.read.parquet(path).count()
    except AnalysisException:
        return 0


def safe_count_rows(
    spark: SparkSession,
    path: str,
    partition_col: str,
    process_date: str,
) -> int:
    """
    Safe for datasets that may not exist yet.
    """
    try:
        return spark.read.parquet(f"{path}/{partition_col}={process_date}").count()
    except AnalysisException:
        return 0


def safe_count_distinct_rows(
    spark: SparkSession,
    path: str,
    partition_col: str,
    process_date: str,
    col_name: str,
) -> int:
    """
    Safe for datasets that may not exist yet.
    """
    try:
        df = spark.read.parquet(f"{path}/{partition_col}={process_date}")
        return df.select(col_name).distinct().count()
    except AnalysisException:
        return 0


def safe_count_invalid_values(
    spark: SparkSession,
    path: str,
    partition_col: str,
    process_date: str,
    col_name: str,
    valid_values: list[str],
) -> int:
    try:
        df = spark.read.parquet(f"{path}/{partition_col}={process_date}")
        return df.filter(~F.col(col_name).isin(valid_values)).count()
    except AnalysisException:
        return 0



def collect_run_metrics(
    spark: SparkSession,
    env_name: str,
    process_date: str,
) -> dict[str, int]:
    config = load_config(env_name)

    raw_logs_path = resolve_output_path(config, "raw_logs")
    normalized_path = resolve_output_path(config, "normalized_events")
    quarantined_raw_lines_path = resolve_output_path(config, "quarantined_raw_lines")
    quarantined_raw_events_path = resolve_output_path(config, "quarantined_raw_events")
    sessionized_path = resolve_output_path(config, "sessionized_events")
    session_features_path = resolve_output_path(config, "session_features")
    session_features_enriched_path = resolve_output_path(config, "session_features_enriched")
    suspicious_path = resolve_output_path(config, "suspicious_sessions")
    daily_summary_path = resolve_output_path(config, "daily_abuse_summary")
    parse_failure_summary_path = resolve_output_path(config, "parse_failure_summary")

    suspicious_df = (
        spark.read.parquet(f"{suspicious_path}/session_date={process_date}")
    )
    band_counts = (
        suspicious_df.groupBy("risk_band")
        .count()
        .collect()
    )

    band_map = {row["risk_band"]: row["count"] for row in band_counts}

    return {
        "raw_partition_rows": count_partition_rows(
            spark,
            raw_logs_path,
            "dt",
            process_date,
        ),

        "normalized_rows": count_partition_rows(
            spark,
            normalized_path,
            "event_date",
            process_date,
        ),
        "quarantined_raw_lines": safe_count_all_rows(
            spark,
            quarantined_raw_lines_path,
        ),

        "parse_error_raw_events": safe_count_rows(
            spark,
            quarantined_raw_events_path,
            "process_date",
            process_date,
        ),

        "parse_error_type_count": safe_count_distinct_rows(
            spark,
            parse_failure_summary_path,
            "process_date",
            process_date,
            "error_type"
        ),

        "sessionized_rows": count_partition_rows(
            spark,
            sessionized_path,
            "session_date",
            process_date,
        ),
        "session_feature_rows": count_partition_rows(
            spark,
            session_features_path,
            "session_date",
            process_date,
        ),
        "session_feature_enriched_rows": count_partition_rows(
            spark,
            session_features_enriched_path,
            "session_date",
            process_date
        ),
        "suspicious_session_rows": count_partition_rows(
            spark,
            suspicious_path,
            "session_date",
            process_date,
        ),
        "flagged_session_rows": count_filtered_partition_rows(
            spark,
            suspicious_path,
            "session_date",
            process_date,
            F.col("risk_band") != "benign",
        ),
        "non_benign_ratio_pct": round(
            100 * suspicious_df.filter(F.col("risk_band") != "benign").count() / max(1, suspicious_df.count()),
            2,
        ),

        # risk band
        "high_risk_rows": band_map.get("high", 0),
        "medium_risk_rows": band_map.get("medium", 0),
        "low_risk_rows": band_map.get("low", 0),
        "benign_rows": band_map.get("benign", 0),

        # dup check
        "normalized_duplicate_rows": count_duplicate_rows(
            spark, normalized_path, "event_date", process_date, "line_hash"
        ),
        "session_feature_duplicate_rows": count_duplicate_rows(
            spark, session_features_path, "session_date", process_date, "session_id"
        ),
        "session_feature_enriched_duplicate_rows": count_duplicate_rows(
            spark, session_features_enriched_path, "session_date", process_date, "session_id"
        ),
        "suspicious_duplicate_rows": count_duplicate_rows(
            spark, suspicious_path, "session_date", process_date, "session_id"
        ),

        # null checks
        "normalized_null_event_time_rows": count_nulls(
            spark, normalized_path, "event_date", process_date, "event_time"
        ),
        "normalized_null_src_ip_rows": count_nulls(
            spark, normalized_path, "event_date", process_date, "src_ip"
        ),
        "normalized_null_method_rows": count_nulls(
            spark, normalized_path, "event_date", process_date, "method"
        ),
        "normalized_null_path_rows": count_nulls(
            spark, normalized_path, "event_date", process_date, "path"
        ),
        "sessionized_null_session_id_rows": count_nulls(
            spark, sessionized_path, "session_date", process_date, "session_id"
        ),

        # value out of range
        "invalid_missing_referer_ratio_rows": count_out_of_range(
            spark, session_features_path, "session_date", process_date,
            "missing_referer_ratio", 0, 1
        ),
        "invalid_status_4xx_ratio_rows": count_out_of_range(
            spark, session_features_path, "session_date", process_date,
            "status_4xx_ratio", 0, 1
        ),
        "invalid_html_to_asset_ratio_rows": count_out_of_range(
            spark, session_features_path, "session_date", process_date,
            "html_to_asset_ratio", 0
        ),

        # invalid values
        "invalid_risk_band_rows": count_invalid_values(
            spark,
            suspicious_path,
            "session_date",
            process_date,
            "risk_band",
            ["benign", "low", "medium", "high"]
        ),
        "invalid_context_tag_rows": safe_count_invalid_values(
            spark,
            suspicious_path,
            "session_date",
            process_date,
            "context_tag",
            ["known_bot_candidate", "unresolved_host", "no_context_signal",]
        ),

        "summary_rows": count_partition_rows(
            spark,
            daily_summary_path,
            "event_date",
            process_date,
        ),
    }