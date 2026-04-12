from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_session_features(sessionized_df):

    df = sessionized_df

    # helper flags
    df = (
        df.withColumn("is_html", F.when(F.col("is_asset") == False, 1).otherwise(0))
          .withColumn("is_asset_req", F.when(F.col("is_asset") == True, 1).otherwise(0))
          .withColumn("is_missing_ref", F.when(F.col("referer").isNull(), 1).otherwise(0))
          .withColumn("is_4xx", F.when((F.col("status_code") >= 400) & (F.col("status_code") < 500), 1).otherwise(0))
    )

    features = (
        df.groupBy("session_id")
        .agg(
            F.first("src_ip").alias("src_ip"),
            F.first("user_agent").alias("user_agent"),

            F.min("session_start_ts").alias("session_start_ts"),
            F.max("session_duration_sec").alias("session_duration_sec"),
            F.max("session_event_count").alias("request_count"),

            F.countDistinct("path").alias("unique_paths"),
            F.countDistinct("path_template").alias("unique_path_templates"),

            F.avg("is_missing_ref").alias("missing_referer_ratio"),
            F.avg("is_4xx").alias("status_4xx_ratio"),

            F.avg("gap_from_prev_request_sec").alias("mean_inter_request_gap"),

            F.sum("is_html").alias("html_count"),
            F.sum("is_asset_req").alias("asset_count"),
        )
        # requests_per_minute
        .withColumn(
            "requests_per_minute",
            F.when(
                F.col("session_duration_sec") > 0,
                F.col("request_count") / (F.col("session_duration_sec") / 60.0)
            ).otherwise(F.col("request_count").cast("double"))
        )
        # html_to_asset_ratio
        .withColumn(
            "html_to_asset_ratio",
            F.when(F.col("asset_count") > 0,
                   F.col("html_count") / F.col("asset_count"))
             .otherwise(F.lit(None).cast("double"))
        )
        # session_date for partitioning
        .withColumn(
            "session_date",
            F.to_date("session_start_ts")
        )
        .select(
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
        )
    )

    return features