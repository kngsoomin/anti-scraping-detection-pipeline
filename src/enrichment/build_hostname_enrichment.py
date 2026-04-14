from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _build_domain_regex(domains: list[str]) -> str:
    escaped = [d.replace(".", r"\.") for d in domains]
    return rf"(?i)\b({'|'.join(escaped)})$"

def _build_ua_regex(patterns: list[str]) -> str:
    return rf"(?i)({'|'.join(patterns)})"

def build_hostname_enrichment(
    session_features_df: DataFrame,
    hostname_df: DataFrame,
    known_bot_domains: list[str],
    known_bot_ua_patterns: list[str],
) -> DataFrame:
    hostname_lookup_df = (
        hostname_df.select(
            F.col("client").alias("src_ip"),
            F.trim(F.col("hostname")).alias("hostname"),
            F.col("alias_list"),
        )
        .filter(
            (F.col("hostname").isNotNull()) &
            (F.length(F.col("hostname")) > 0) &
            (F.col("alias_list") != "[Errno 1] Unknown host")
        )
        .dropDuplicates(["src_ip"])
    )

    domain_regex = _build_domain_regex(known_bot_domains)
    ua_regex = _build_ua_regex(known_bot_ua_patterns)

    enriched_df = (
        session_features_df.alias("sf")
        .join(hostname_lookup_df.alias("hl"), on="src_ip", how="left")
        .withColumn(
            "has_hostname",
            F.when(
                F.col("hostname").isNotNull(),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn(
            "is_known_bot_domain",
            F.when(
                F.col("has_hostname") &
                F.col("hostname").rlike(domain_regex),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn(
            "is_known_bot_ua",
            F.when(
                F.col("user_agent").rlike(ua_regex),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    )

    return enriched_df.select(
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
        "is_known_bot_ua"
    )