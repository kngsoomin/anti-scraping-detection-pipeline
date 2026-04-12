from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)


SESSION_FEATURES_SCHEMA = StructType([
    StructField("session_id", StringType(), True),

    StructField("src_ip", StringType(), True),
    StructField("user_agent", StringType(), True),

    StructField("request_count", LongType(), True),
    StructField("requests_per_minute", DoubleType(), True),
    StructField("session_duration_sec", LongType(), True),

    StructField("unique_paths", LongType(), True),
    StructField("unique_path_templates", LongType(), True),

    StructField("missing_referer_ratio", DoubleType(), True),
    StructField("html_to_asset_ratio", DoubleType(), True),

    StructField("status_4xx_ratio", DoubleType(), True),
    StructField("mean_inter_request_gap", DoubleType(), True),
])