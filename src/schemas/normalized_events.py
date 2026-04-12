from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType
)

NORMALIZED_EVENTS_SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("src_ip", StringType(), True),
    StructField("method", StringType(), True),
    StructField("raw_path", StringType(), True),
    StructField("path", StringType(), True),
    StructField("path_template", StringType(), True),
    StructField("query_string", StringType(), True),
    StructField("protocol", StringType(), True),
    StructField("status_code", LongType(), True),
    StructField("bytes_sent", LongType(), True),
    StructField("referer", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("is_asset", BooleanType(), True),
    StructField("asset_type", StringType(), True),
    StructField("is_known_bot_ua", BooleanType(), True),
    StructField("is_robots_request", BooleanType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("line_hash", StringType(), True),
])