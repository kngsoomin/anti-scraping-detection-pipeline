from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_daily_abuse_summary(suspicious_sessions_df):
    df = suspicious_sessions_df

    df = (
        df.withColumn("session_start_ts", F.to_timestamp("session_start_ts"))
          .withColumnRenamed("session_date", "event_date")
          .withColumn("event_hour", F.hour("session_start_ts"))
    )

    # daily summary: basic
    daily_counts_df = (
        df.groupBy("event_date")
        .agg(
            F.count("*").alias("suspicious_session_count"),
            F.sum(F.when(F.col("risk_band") == "high", 1).otherwise(0)).alias("high_risk_session_count"),
            F.sum(F.when(F.col("risk_band") == "medium", 1).otherwise(0)).alias("medium_risk_session_count"),
            F.sum(F.when(F.col("risk_band") == "low", 1).otherwise(0)).alias("low_risk_session_count"),
        )
    )

    # top flagged IPs
    top_ips_df = (
        df.groupBy("event_date", "src_ip")
        .agg(F.count("*").alias("session_count"))
        .withColumn(
            "ip_rank",
            F.row_number().over(
                Window.partitionBy("event_date").orderBy(F.desc("session_count"), F.asc("src_ip"))
            )
        )
        .filter(F.col("ip_rank") <= 5)
        .groupBy("event_date")
        .agg(
            F.collect_list(
                F.struct(
                    F.col("src_ip"),
                    F.col("session_count")
                )
            ).alias("top_flagged_ips")
        )
    )

    # risk band distribution
    risk_dist_df = (
        df.groupBy("event_date", "risk_band")
        .agg(F.count("*").alias("session_count"))
        .groupBy("event_date")
        .agg(
            F.collect_list(
                F.struct(
                    F.col("risk_band"),
                    F.col("session_count")
                )
            ).alias("risk_band_distribution")
        )
    )

    summary_df = (
        daily_counts_df
        .join(top_ips_df, on="event_date", how="left")
        .join(risk_dist_df, on="event_date", how="left")
        .orderBy("event_date")
    )

    return summary_df