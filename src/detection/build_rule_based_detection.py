from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_rule_based_detection(
        session_features_df,
        thresholds: dict,
        risk_bands: dict
):
    df = session_features_df

    df = (
        df.withColumn(
            "flag_high_requests_per_minute",
            F.when(F.col("requests_per_minute") >= thresholds["requests_per_minute"], 1).otherwise(0)
        )
        .withColumn(
            "flag_high_unique_paths",
            F.when(F.col("unique_paths") >= thresholds["unique_paths"], 1).otherwise(0)
        )
        .withColumn(
            "flag_high_unique_path_templates",
            F.when(F.col("unique_path_templates") >= thresholds["unique_path_templates"], 1).otherwise(0)
        )
        .withColumn(
            "flag_high_missing_referer_ratio",
            F.when(F.col("missing_referer_ratio") >= thresholds["missing_referer_ratio"], 1).otherwise(0)
        )
        .withColumn(
            "flag_high_html_to_asset_ratio",
            F.when(F.col("html_to_asset_ratio") >= thresholds["html_to_asset_ratio"], 1).otherwise(0)
        )
        .withColumn(
            "flag_high_4xx_ratio",
            F.when(F.col("status_4xx_ratio") >= thresholds["status_4xx_ratio"], 1).otherwise(0)
        )
        .withColumn(
            "flag_low_mean_inter_request_gap",
            F.when(F.col("mean_inter_request_gap") <= thresholds["mean_inter_request_gap_max"], 1).otherwise(0)
        )
    )

    df = df.withColumn(
        "rule_score",
        F.col("flag_high_requests_per_minute")
        + F.col("flag_high_unique_paths")
        + F.col("flag_high_unique_path_templates")
        + F.col("flag_high_missing_referer_ratio")
        + F.col("flag_high_html_to_asset_ratio")
        + F.col("flag_high_4xx_ratio")
        + F.col("flag_low_mean_inter_request_gap")
    )

    reasons = [
        ("high_requests_per_minute", "flag_high_requests_per_minute"),
        ("high_unique_paths", "flag_high_unique_paths"),
        ("high_unique_path_templates", "flag_high_unique_path_templates"),
        ("high_missing_referer_ratio", "flag_high_missing_referer_ratio"),
        ("high_html_to_asset_ratio", "flag_high_html_to_asset_ratio"),
        ("high_4xx_ratio", "flag_high_4xx_ratio"),
        ("low_mean_inter_request_gap", "flag_low_mean_inter_request_gap"),
    ]

    df = df.withColumn(
        "top_reasons",
        F.array(*[
            F.when(F.col(flag) == 1, reason)
            for reason, flag in reasons
        ])
    )

    df = df.withColumn(
        "top_reasons",
        F.filter(F.col("top_reasons"), lambda x: x.isNotNull())
    )

    df = df.withColumn(
        "risk_band",
        F.when(F.col("rule_score") >= risk_bands['high_min_score'], F.lit("high"))
         .when(F.col("rule_score") >= risk_bands['medium_min_score'], F.lit("medium"))
         .when(F.col("rule_score") >= risk_bands['low_min_score'], F.lit("low"))
         .otherwise(F.lit("benign"))
    )

    return df.select(
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
        "rule_score",
        "risk_band",
        "top_reasons",
    )